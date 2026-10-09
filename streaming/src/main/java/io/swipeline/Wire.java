package io.swipeline;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.*;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.*;
public final class Wire {
 public static final ObjectMapper JSON=new ObjectMapper();
 public static ObjectNode parse(String s) throws Exception {
  JsonNode n=JSON.readTree(s);if(!n.isObject())throw new IllegalArgumentException("object required");return (ObjectNode)n;
 }
 public static long integer(ObjectNode n,String key) {
  JsonNode v=n.get(key);if(v==null||!v.isIntegralNumber()||!v.canConvertToLong())throw new IllegalArgumentException(key);
  return v.longValue();
 }
 public static String id(String... parts) throws Exception {
  String json=JSON.writeValueAsString(parts).replace("&","\\u0026").replace("<","\\u003c").replace(">","\\u003e");
  return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(json.getBytes(StandardCharsets.UTF_8)));
 }
 public static ObjectNode empty(){ObjectNode n=JSON.createObjectNode();n.put("version",1);return n;}
}
