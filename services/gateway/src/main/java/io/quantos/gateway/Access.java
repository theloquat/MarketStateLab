package io.quantos.gateway;

import com.fasterxml.jackson.databind.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public final class Access {
  static final ObjectMapper JSON = new ObjectMapper();

  public record Claims(String user, String role, String jti, long expires) {}

  public static Claims verify(String token, String secret, long now) {
    try {
      String[] parts = token.split("\\.");
      if (parts.length != 3) throw new SecurityException();
      JsonNode header = JSON.readTree(Base64.getUrlDecoder().decode(parts[0]));
      if (!"HS256".equals(header.path("alg").asText())) throw new SecurityException();
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
      byte[] expected = mac.doFinal((parts[0] + "." + parts[1]).getBytes(StandardCharsets.UTF_8));
      if (!MessageDigest.isEqual(expected, Base64.getUrlDecoder().decode(parts[2])))
        throw new SecurityException();
      JsonNode c = JSON.readTree(Base64.getUrlDecoder().decode(parts[1]));
      if (c.path("exp").asLong() <= now
          || c.path("iat").asLong() > now + 30
          || !"quantos".equals(c.path("iss").asText())
          || !"market-lab".equals(c.path("aud").asText())
          || !Set.of("viewer", "researcher", "admin").contains(c.path("role").asText())
          || c.path("sub").asText().isEmpty()
          || c.path("jti").asText().isEmpty()) throw new SecurityException();
      return new Claims(
          c.path("sub").asText(),
          c.path("role").asText(),
          c.path("jti").asText(),
          c.path("exp").asLong());
    } catch (Exception e) {
      throw new SecurityException("Invalid or expired session");
    }
  }

  public static boolean canWrite(String role, String path, String method) {
    if (path.startsWith("/api/lab/admin/") && !role.equals("admin")) return false;
    if (Set.of("GET", "HEAD", "OPTIONS").contains(method) || path.equals("/graphql")) return true;
    if (path.startsWith("/auth/")) return true;
    if (role.equals("viewer")) return false;
    return !path.startsWith("/api/lab/admin/") || role.equals("admin");
  }

  public static boolean publicPath(String p) {
    return p.equals("/health")
        || p.equals("/lab")
        || p.equals("/")
        || p.startsWith("/assets/")
        || p.startsWith("/vendor/")
        || p.equals("/auth/login");
  }

  public static String backend(String path) {
    if (path.startsWith("/api/features/") || path.startsWith("/api/models/"))
      return "http://feature-model:8000";
    if (path.startsWith("/api/alerts/")) return "http://alerts:8000";
    return "http://data-api:8000";
  }
}
