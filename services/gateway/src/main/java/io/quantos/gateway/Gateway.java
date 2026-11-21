package io.quantos.gateway;

import com.github.benmanes.caffeine.cache.*;
import io.undertow.*;
import io.undertow.server.*;
import io.undertow.util.*;
import io.undertow.websockets.core.*;
import java.net.*;
import java.net.http.*;
import java.nio.*;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import redis.clients.jedis.*;

public final class Gateway {
  record Reply(int code, String type, byte[] body) {}

  final HttpClient http =
      HttpClient.newBuilder()
          .version(HttpClient.Version.HTTP_1_1)
          .connectTimeout(Duration.ofSeconds(3))
          .build();
  final Cache<String, Reply> responses =
      Caffeine.newBuilder()
          .maximumWeight(16 * 1024 * 1024)
          .weigher((String k, Reply r) -> r.body().length + k.length() * 2 + 128)
          .expireAfterWrite(Duration.ofMillis(300))
          .recordStats()
          .build();
  final Cache<String, AtomicInteger> rates =
      Caffeine.newBuilder().maximumSize(4096).expireAfterWrite(Duration.ofSeconds(1)).build();
  final Cache<String, AtomicInteger> logins =
      Caffeine.newBuilder().maximumSize(1024).expireAfterWrite(Duration.ofMinutes(1)).build();
  final Predicate<String> revoked;
  final Function<String, String> route;
  final String jwt, internal;

  Gateway() {
    this(
        Objects.requireNonNull(System.getenv("CONTEST_JWT_SECRET")),
        Objects.requireNonNull(System.getenv("CONTEST_INTERNAL_TOKEN")),
        new RedisRevocations(),
        Access::backend);
  }

  Gateway(String jwt, String internal, Predicate<String> revoked, Function<String, String> route) {
    this.jwt = jwt;
    this.internal = internal;
    this.revoked = revoked;
    this.route = route;
    if (jwt.length() < 32 || internal.length() < 32)
      throw new IllegalArgumentException("Secrets must contain at least 32 characters");
  }

  static class RedisRevocations implements Predicate<String> {
    final JedisPool pool;

    RedisRevocations() {
      var config = new JedisPoolConfig();
      config.setMaxTotal(32);
      config.setMaxIdle(32);
      config.setMinIdle(4);
      config.setMaxWait(Duration.ofSeconds(2));
      pool = new JedisPool(config, "redis", 6379, 2000);
    }

    public boolean test(String id) {
      try (Jedis j = pool.getResource()) {
        return j.exists("quantos:revoked:" + id);
      }
    }
  }

  static final AttachmentKey<Long> START = AttachmentKey.create(Long.class);
  final AtomicLong requests = new AtomicLong(),
      errors = new AtomicLong(),
      limited = new AtomicLong();
  final Set<WebSocketChannel> channels = ConcurrentHashMap.newKeySet();

  public static void main(String[] args) {
    java.security.Security.setProperty("networkaddress.cache.ttl", "1");
    java.security.Security.setProperty("networkaddress.cache.negative.ttl", "0");
    System.setProperty("jdk.httpclient.keepalive.timeout", "2");
    Gateway g = new Gateway();
    Undertow.builder()
        .addHttpListener(8080, "0.0.0.0")
        .setIoThreads(2)
        .setWorkerThreads(16)
        .setServerOption(UndertowOptions.MAX_ENTITY_SIZE, 12L * 1024 * 1024)
        .setHandler(
            e -> {
              e.putAttachment(START, System.nanoTime());
              e.dispatch(() -> g.handle(e));
            })
        .build()
        .start();
  }

  void json(HttpServerExchange e, int code, String value) {
    e.setStatusCode(code);
    e.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json; charset=utf-8");
    e.getResponseHeaders().put(new HttpString("Cache-Control"), "no-store");
    e.getResponseSender().send(value);
  }

  String token(HttpServerExchange e) {
    String a = e.getRequestHeaders().getFirst(Headers.AUTHORIZATION);
    if (a != null && a.startsWith("Bearer ")) return a.substring(7);
    var cookie = e.getRequestCookies().get("quantos_session");
    return cookie == null ? "" : cookie.getValue();
  }

  Access.Claims authenticate(HttpServerExchange e) {
    long t = System.nanoTime();
    Access.Claims c = Access.verify(token(e), jwt, Instant.now().getEpochSecond());
    long verified = System.nanoTime();
    if (revoked.test(c.jti())) throw new SecurityException();
    e.getResponseHeaders()
        .put(new HttpString("X-Auth-Ms"), Double.toString((System.nanoTime() - t) / 1e6));
    e.getResponseHeaders()
        .put(
            new HttpString("X-Revocation-Ms"),
            Double.toString((System.nanoTime() - verified) / 1e6));
    return c;
  }

  void handle(HttpServerExchange e) {
    if (e.getAttachment(START) == null) e.putAttachment(START, System.nanoTime());
    requests.incrementAndGet();
    e.getResponseHeaders()
        .put(
            new HttpString("X-Queue-Ms"),
            Double.toString((System.nanoTime() - e.getAttachment(START)) / 1e6));
    String path = e.getRequestPath(), method = e.getRequestMethod().toString();
    try {
      String host = e.getRequestHeaders().getFirst(Headers.HOST),
          origin = e.getRequestHeaders().getFirst(Headers.ORIGIN);
      if (host == null
          || !(host.matches("(localhost|127\\.0\\.0\\.1)(:[0-9]+)?")
              || host.equals("gateway:8080"))) {
        json(e, 400, "{\"detail\":\"Invalid host\"}");
        return;
      }
      if (origin != null
          && (!Set.of("GET", "HEAD").contains(method) || path.equals("/api/lab/events"))
          && !URI.create(origin).getAuthority().equals(host)) {
        json(e, 403, "{\"detail\":\"Origin rejected\"}");
        return;
      }
      Access.Claims claims = null;
      if (!Access.publicPath(path)) {
        try {
          claims = authenticate(e);
        } catch (SecurityException bad) {
          json(e, 401, "{\"detail\":\"请登录市场状态实验室\"}");
          return;
        }
        if (!Access.canWrite(claims.role(), path, method)) {
          json(e, 403, "{\"detail\":\"当前角色只有读取权限\"}");
          return;
        }
        int count = rates.get(claims.user(), k -> new AtomicInteger()).incrementAndGet();
        if (count > 80) {
          limited.incrementAndGet();
          e.getResponseHeaders().put(new HttpString("Retry-After"), "1");
          json(e, 429, "{\"detail\":\"请求过于频繁\"}");
          return;
        }
      }
      if (path.equals("/auth/login")
          && logins
                  .get(e.getSourceAddress().getAddress().getHostAddress(), k -> new AtomicInteger())
                  .incrementAndGet()
              > 10) {
        limited.incrementAndGet();
        json(e, 429, "{\"detail\":\"登录尝试过多，请稍后重试\"}");
        return;
      }
      if (path.equals("/health")) {
        json(e, 200, "{\"status\":\"ok\",\"service\":\"java-gateway\"}");
        return;
      }
      if (path.equals("/api/gateway/metrics")) {
        if (!claims.role().equals("admin")) {
          json(e, 403, "{}");
          return;
        }
        json(
            e,
            200,
            "{\"requests\":"
                + requests.get()
                + ",\"errors\":"
                + errors.get()
                + ",\"rateLimited\":"
                + limited.get()
                + ",\"cacheHits\":"
                + responses.stats().hitCount()
                + ",\"cacheMisses\":"
                + responses.stats().missCount()
                + ",\"websockets\":"
                + channels.size()
                + "}");
        return;
      }
      if (path.equals("/api/lab/events")) {
        websocket(e, token(e), origin, host);
        return;
      }
      e.startBlocking();
      byte[] body = e.getInputStream().readNBytes(12 * 1024 * 1024 + 1);
      if (body.length > 12 * 1024 * 1024) {
        json(e, 413, "{\"detail\":\"请求过大\"}");
        return;
      }
      String query = e.getQueryString().isEmpty() ? "" : "?" + e.getQueryString();
      boolean dashboard =
          method.equals("POST")
              && path.equals("/graphql")
              && new String(body, StandardCharsets.UTF_8)
                  .replaceAll("\\s", "")
                  .equals("{\"query\":\"{dashboard}\"}");
      boolean cacheable =
          dashboard
              || method.equals("GET")
                  && (path.startsWith("/api/features/")
                      || path.startsWith("/api/models/")
                      || path.startsWith("/api/alerts/")
                      || path.equals("/api/lab/summary"));
      String key = (claims == null ? "public" : claims.role()) + ":" + path + query;
      Reply cached = cacheable ? responses.getIfPresent(key) : null;
      if (cached != null) {
        e.getResponseHeaders().put(new HttpString("X-Cache"), "CAFFEINE");
        reply(e, cached);
        return;
      }
      if (cacheable) {
        String content = e.getRequestHeaders().getFirst(Headers.CONTENT_TYPE);
        Reply loaded =
            responses.get(
                key, k -> cachedRead(path, query, method, body, host, token(e), origin, content));
        if (loaded.code() != 200) responses.invalidate(key);
        reply(e, loaded);
        return;
      }
      HttpRequest.Builder b =
          HttpRequest.newBuilder(URI.create(route.apply(path) + path + query))
              .timeout(Duration.ofSeconds(40))
              .header("X-Internal-Token", internal)
              .header("X-Forwarded-Host", host)
              .header("X-Request-Id", UUID.randomUUID().toString());
      String content = e.getRequestHeaders().getFirst(Headers.CONTENT_TYPE);
      if (content != null) b.header("Content-Type", content);
      if (claims != null) b.header("Authorization", "Bearer " + token(e));
      if (origin != null) b.header("Origin", origin);
      b.method(
          method,
          body.length == 0
              ? HttpRequest.BodyPublishers.noBody()
              : HttpRequest.BodyPublishers.ofByteArray(body));
      HttpResponse<byte[]> r = http.send(b.build(), HttpResponse.BodyHandlers.ofByteArray());
      r.headers()
          .allValues("set-cookie")
          .forEach(v -> e.getResponseHeaders().add(Headers.SET_COOKIE, v));
      r.headers()
          .firstValue("location")
          .ifPresent(v -> e.getResponseHeaders().put(Headers.LOCATION, v));
      Reply response =
          new Reply(
              r.statusCode(),
              r.headers().firstValue("content-type").orElse("application/json"),
              r.body());
      if (cacheable && r.statusCode() == 200) responses.put(key, response);
      if (!Set.of("GET", "HEAD", "OPTIONS").contains(method) && !path.equals("/graphql"))
        responses.invalidateAll();
      reply(e, response);
    } catch (Exception error) {
      errors.incrementAndGet();
      json(e, 503, "{\"detail\":\"服务暂不可用，请查看运行监控\"}");
    }
  }

  Reply cachedRead(
      String path,
      String query,
      String method,
      byte[] body,
      String host,
      String token,
      String origin,
      String content) {
    try {
      var b =
          HttpRequest.newBuilder(URI.create(route.apply(path) + path + query))
              .timeout(Duration.ofSeconds(15))
              .header("X-Internal-Token", internal)
              .header("X-Forwarded-Host", host)
              .header("Authorization", "Bearer " + token);
      if (origin != null) b.header("Origin", origin);
      if (content != null) b.header("Content-Type", content);
      b.method(
          method,
          body.length == 0
              ? HttpRequest.BodyPublishers.noBody()
              : HttpRequest.BodyPublishers.ofByteArray(body));
      var r = http.send(b.build(), HttpResponse.BodyHandlers.ofByteArray());
      return new Reply(
          r.statusCode(),
          r.headers().firstValue("content-type").orElse("application/json"),
          r.body());
    } catch (Exception error) {
      throw new IllegalStateException("Backend unavailable");
    }
  }

  void reply(HttpServerExchange e, Reply r) {
    e.setStatusCode(r.code());
    Long started = e.getAttachment(START);
    if (started != null)
      e.getResponseHeaders()
          .put(
              new HttpString("Server-Timing"),
              "gateway;dur=" + ((System.nanoTime() - started) / 1e6));
    e.getResponseHeaders().put(Headers.CONTENT_TYPE, r.type());
    e.getResponseHeaders().put(new HttpString("X-Content-Type-Options"), "nosniff");
    e.getResponseSender().send(ByteBuffer.wrap(r.body()));
  }

  void websocket(HttpServerExchange e, String token, String origin, String host) throws Exception {
    Handlers.websocket(
            (exchange, channel) -> {
              channels.add(channel);
              channel.addCloseTask(c -> channels.remove(c));
              var listener =
                  new java.net.http.WebSocket.Listener() {
                    final StringBuilder message = new StringBuilder();

                    public void onOpen(java.net.http.WebSocket ws) {
                      ws.request(1);
                    }

                    public CompletionStage<?> onText(
                        java.net.http.WebSocket ws, CharSequence data, boolean last) {
                      message.append(data);
                      if (message.length() > 1024 * 1024) {
                        ws.abort();
                        try {
                          channel.close();
                        } catch (Exception ignored) {
                        }
                        return null;
                      }
                      if (last) {
                        String text = message.toString();
                        message.setLength(0);
                        WebSockets.sendText(
                            text,
                            channel,
                            new WebSocketCallback<Void>() {
                              public void complete(WebSocketChannel c, Void v) {
                                ws.request(1);
                              }

                              public void onError(WebSocketChannel c, Void v, Throwable t) {
                                ws.abort();
                              }
                            });
                      } else ws.request(1);
                      return null;
                    }

                    public CompletionStage<?> onClose(
                        java.net.http.WebSocket ws, int code, String reason) {
                      try {
                        channel.close();
                      } catch (Exception ignored) {
                      }
                      return null;
                    }

                    public void onError(java.net.http.WebSocket ws, Throwable error) {
                      try {
                        channel.close();
                      } catch (Exception ignored) {
                      }
                    }
                  };
              http.newWebSocketBuilder()
                  .header("Authorization", "Bearer " + token)
                  .header("X-Internal-Token", internal)
                  .header("X-Forwarded-Host", host)
                  .header("Origin", origin == null ? "http://" + host : origin)
                  .buildAsync(
                      URI.create(
                          route.apply("/api/lab/events").replaceFirst("^http", "ws")
                              + "/api/lab/events"),
                      listener)
                  .whenComplete(
                      (upstream, error) -> {
                        if (error != null) {
                          try {
                            channel.close();
                          } catch (Exception ignored) {
                          }
                          return;
                        }
                        channel.addCloseTask(c -> upstream.sendClose(1000, "closed"));
                        channel
                            .getReceiveSetter()
                            .set(
                                new AbstractReceiveListener() {
                                  protected void onFullTextMessage(
                                      WebSocketChannel c, BufferedTextMessage m) {
                                    if (m.getData().length() > 8192) {
                                      upstream.abort();
                                      return;
                                    }
                                    upstream.sendText(m.getData(), true);
                                  }
                                });
                        channel.resumeReceives();
                      });
            })
        .handleRequest(e);
  }
}
