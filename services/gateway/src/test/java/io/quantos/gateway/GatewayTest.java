package io.quantos.gateway;

import static org.junit.jupiter.api.Assertions.*;

import io.undertow.*;
import io.undertow.util.*;
import io.undertow.websockets.core.*;
import java.net.*;
import java.net.http.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.*;

class GatewayTest {
  static final String SECRET = "TEST-ONLY-SECRET-0000000000000000000000000000";
  Undertow backend, front;
  String base;
  Gateway gateway;
  AtomicInteger calls;

  String token(String user, String role, long expires) throws Exception {
    var enc = Base64.getUrlEncoder().withoutPadding();
    String h = enc.encodeToString("{\"alg\":\"HS256\"}".getBytes());
    String p =
        enc.encodeToString(
            ("{\"sub\":\""
                    + user
                    + "\",\"role\":\""
                    + role
                    + "\",\"jti\":\"test-jti\",\"iat\":0,\"exp\":"
                    + expires
                    + ",\"iss\":\"quantos\",\"aud\":\"market-lab\"}")
                .getBytes());
    Mac m = Mac.getInstance("HmacSHA256");
    m.init(new SecretKeySpec(SECRET.getBytes(), "HmacSHA256"));
    return h + "." + p + "." + enc.encodeToString(m.doFinal((h + "." + p).getBytes()));
  }

  @BeforeEach
  void start() {
    calls = new AtomicInteger();
    backend =
        Undertow.builder()
            .addHttpListener(0, "127.0.0.1")
            .setIoThreads(1)
            .setWorkerThreads(2)
            .setHandler(
                e -> {
                  if (e.getRequestPath().equals("/api/lab/events")) {
                    Handlers.websocket(
                            (x, ch) -> WebSockets.sendText("{\"type\":\"connected\"}", ch, null))
                        .handleRequest(e);
                    return;
                  }
                  calls.incrementAndGet();
                  e.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                  e.getResponseSender().send("{\"value\":42}");
                })
            .build();
    backend.start();
    int bp = ((InetSocketAddress) backend.getListenerInfo().get(0).getAddress()).getPort();
    gateway = new Gateway(SECRET, SECRET, x -> false, x -> "http://127.0.0.1:" + bp);
    front =
        Undertow.builder()
            .addHttpListener(0, "127.0.0.1")
            .setIoThreads(1)
            .setWorkerThreads(4)
            .setHandler(e -> e.dispatch(() -> gateway.handle(e)))
            .build();
    front.start();
    base =
        "http://127.0.0.1:"
            + ((InetSocketAddress) front.getListenerInfo().get(0).getAddress()).getPort();
  }

  @AfterEach
  void stop() {
    front.stop();
    backend.stop();
  }

  HttpResponse<String> request(String path, String method, String token, String origin)
      throws Exception {
    var b =
        HttpRequest.newBuilder(URI.create(base + path)).timeout(java.time.Duration.ofSeconds(5));
    if (token != null) b.header("Authorization", "Bearer " + token);
    if (origin != null) b.header("Origin", origin);
    b.method(
        method,
        method.equals("POST")
            ? HttpRequest.BodyPublishers.ofString("{}")
            : HttpRequest.BodyPublishers.noBody());
    return HttpClient.newHttpClient().send(b.build(), HttpResponse.BodyHandlers.ofString());
  }

  @Test
  void signaturesExpiryAndPermissions() throws Exception {
    String t = token("viewer", "viewer", System.currentTimeMillis() / 1000 + 60);
    assertEquals("viewer", Access.verify(t, SECRET, System.currentTimeMillis() / 1000).role());
    assertThrows(SecurityException.class, () -> Access.verify(t + "x", SECRET, 1));
    assertThrows(SecurityException.class, () -> Access.verify(t, SECRET, Long.MAX_VALUE));
    assertFalse(Access.canWrite("viewer", "/api/lab/train", "POST"));
    assertFalse(Access.canWrite("researcher", "/api/lab/admin/audit", "GET"));
    assertEquals("http://alerts:8000", Access.backend("/api/alerts/x"));
    assertEquals("http://feature-model:8000", Access.backend("/api/features/latest/x"));
  }

  @Test
  void actualHttpAuthenticationCachingInvalidationAndRateLimit() throws Exception {
    String t = token("viewer", "viewer", System.currentTimeMillis() / 1000 + 60);
    String admin = token("admin", "admin", System.currentTimeMillis() / 1000 + 60);
    assertEquals(200, request("/health", "GET", null, null).statusCode());
    assertEquals(401, request("/api/features/latest/x", "GET", null, null).statusCode());
    assertEquals(403, request("/api/lab/train", "POST", t, null).statusCode());
    assertEquals(
        403, request("/api/lab/train", "POST", admin, "https://evil.example").statusCode());
    var first = request("/api/features/latest/x", "GET", t, null);
    assertEquals(200, first.statusCode());
    assertTrue(first.body().contains("42"));
    int n = calls.get();
    assertEquals(
        "CAFFEINE",
        request("/api/features/latest/x", "GET", t, null)
            .headers()
            .firstValue("X-Cache")
            .orElse(""));
    assertEquals(n, calls.get());
    assertEquals(200, request("/api/lab/train", "POST", admin, null).statusCode());
    request("/api/features/latest/x", "GET", t, null);
    assertTrue(calls.get() > n);
    assertEquals(403, request("/api/gateway/metrics", "GET", t, null).statusCode());
    assertEquals(200, request("/api/gateway/metrics", "GET", admin, null).statusCode());
    gateway.rates.put("viewer", new AtomicInteger(80));
    assertEquals(429, request("/api/features/latest/x", "GET", t, null).statusCode());
  }

  @Test
  void websocketActuallyBridges() throws Exception {
    var received = new CompletableFuture<String>();
    String t = token("viewer", "viewer", System.currentTimeMillis() / 1000 + 60);
    var socket =
        HttpClient.newHttpClient()
            .newWebSocketBuilder()
            .header("Authorization", "Bearer " + t)
            .buildAsync(
                URI.create(base.replace("http:", "ws:") + "/api/lab/events"),
                new java.net.http.WebSocket.Listener() {
                  public void onOpen(java.net.http.WebSocket ws) {
                    ws.request(1);
                  }

                  public CompletionStage<?> onText(
                      java.net.http.WebSocket ws, CharSequence d, boolean last) {
                    received.complete(d.toString());
                    ws.request(1);
                    return null;
                  }
                })
            .get(5, TimeUnit.SECONDS);
    assertTrue(received.get(5, TimeUnit.SECONDS).contains("connected"));
    socket.sendClose(1000, "test done").get(5, TimeUnit.SECONDS);
  }
}
