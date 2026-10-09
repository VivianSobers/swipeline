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
public static void validate(ObjectNode n)throws Exception {
 for(String k:new String[]{"schema_version","event_id","run_id","transaction_id","source_seq","event_time_ms","produced_at_ms","payload_hash","card_token","merchant_id","mcc","issuer_bank_id","acquirer_bank_id","currency","amount_minor","authorization_date","rule_version","reference_version","received_at_ms","scoring_deadline_at_ms","response_deadline_at_ms","deadline_mode","seal_epoch","close_day","accepted_card_seq"})
  if(!n.hasNonNull(k))throw new IllegalArgumentException("missing "+k);
 for(String k:new String[]{"schema_version","source_seq","event_time_ms","produced_at_ms","amount_minor","received_at_ms","scoring_deadline_at_ms","response_deadline_at_ms","seal_epoch","accepted_card_seq"})integer(n,k);
 if(integer(n,"schema_version")!=1||integer(n,"amount_minor")<=0||integer(n,"source_seq")<0||!"USD".equals(n.path("currency").asText()))throw new IllegalArgumentException("invalid contract");
 long day=java.time.LocalDate.parse(n.path("authorization_date").asText()).atStartOfDay(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();
 if(integer(n,"event_time_ms")<day||integer(n,"event_time_ms")>=day+86400000)throw new IllegalArgumentException("event day");
 if(!id(n.path("transaction_id").asText(),"authorization","1").equals(n.path("event_id").asText()))throw new IllegalArgumentException("event id");
 ObjectNode business=JSON.createObjectNode();
 for(String k:new String[]{"run_id","transaction_id","source_seq","event_time_ms","card_token","merchant_id","mcc","issuer_bank_id","acquirer_bank_id","currency","amount_minor","authorization_date","rule_version","reference_version"})business.set(k,n.get(k));
 String canonical=JSON.writeValueAsString(business).replace("&","\\u0026").replace("<","\\u003c").replace(">","\\u003e");
 String hash=HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8)));
 if(!hash.equals(n.path("payload_hash").asText()))throw new IllegalArgumentException("payload hash");
}
public static String quarantine(byte[] input,int partition,long offset,String error,long now)throws Exception {
 ObjectNode q=JSON.createObjectNode();q.put("origin_topic","swipeline.auth.requests.v1");
 q.put("partition",partition);q.put("offset",offset);q.put("error_code",error);
 q.put("payload_hash",HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(input)));
 q.put("sanitized_input","redacted");q.put("detected_at_ms",now);return q.toString();
}
}
