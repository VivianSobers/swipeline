package io.swipeline;

import java.util.HashSet;
import java.util.Set;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

/** Validates immutable finals without changing their payload. */
public final class Decision {
 private Decision() {}
 private static final String[] FEATURES = {"proposal_id", "score", "velocity_60s", "usual_minor", "spend_ratio", "merchant_distinct_estimate", "features_late"};
 public static ObjectNode parse(byte[] raw) {
  try {
   if (raw == null || raw.length > 65536) throw invalid();
   JsonNode tree = Wire.JSON.reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readTree(raw);
   if (tree == null || !tree.isObject()) throw invalid();
   ObjectNode n = (ObjectNode) tree;
   for (String k : new String[]{"event_id", "transaction_id", "payload_hash", "decision_id"}) text(n,k,"[0-9a-f]{64}");
   for (String k : new String[]{"run_id", "merchant_id", "issuer_bank_id", "acquirer_bank_id", "rule_version", "reference_version"}) text(n,k,"[A-Za-z0-9][A-Za-z0-9_-]{0,63}");
   text(n,"card_token","card_[0-9a-f]{64}"); text(n,"mcc","[0-9]{4}"); text(n,"currency","USD");
   text(n,"deadline_mode","latency|throughput");
   for (String k : new String[]{"authorization_date", "close_day"}) {
    text(n,k,"[0-9]{4}-[0-9]{2}-[0-9]{2}"); java.time.LocalDate.parse(n.get(k).textValue());
   }
   for (String k : new String[]{"schema_version", "source_seq", "event_time_ms", "produced_at_ms", "received_at_ms", "scoring_deadline_at_ms", "response_deadline_at_ms", "seal_epoch", "accepted_card_seq", "finalized_at_ms"})
    if (Wire.integer(n,k) < 0) throw invalid();
   if (Wire.integer(n,"schema_version") != 1 || Wire.integer(n,"amount_minor") <= 0) throw invalid();
   if (!Wire.id(transactionId(n),"decision").equals(decisionId(n))) throw invalid();
   text(n,"decision","APPROVE|DECLINE"); text(n,"decision_source","FLINK|STAND_IN"); text(n,"reason","[A-Z][A-Z_]{0,63}");
   number(n,"decision_latency_ms");
   JsonNode flags = n.get("fraud_flags");
   if (flags == null || !flags.isArray()) throw invalid();
   Set<String> seen = new HashSet<>();
   for (JsonNode flag : flags)
    if (!flag.isTextual() || !Set.of("CARD_TESTING","CARD_VELOCITY","SPEND_ANOMALY").contains(flag.textValue()) || !seen.add(flag.textValue())) throw invalid();
   for (String k : FEATURES) if (!n.has(k)) throw invalid();
   if (n.get("decision_source").textValue().equals("STAND_IN")) {
    for (String k : FEATURES) if (!n.get(k).isNull()) throw invalid();
    if (!flags.isEmpty()) throw invalid();
    text(n,"decision","DECLINE"); text(n,"reason","SCORING_UNAVAILABLE|SCORING_TIMEOUT|NETWORK_UNAVAILABLE");
   } else {
    text(n,"proposal_id",".+");
    long score = Wire.integer(n,"score");
    if (score < 0 || score > 180 || Wire.integer(n,"velocity_60s") < 0 || Wire.integer(n,"usual_minor") < 100) throw invalid();
    number(n,"spend_ratio"); number(n,"merchant_distinct_estimate");
    if (!n.get("features_late").isBoolean()) throw invalid();
   }
   return n;
  } catch (Exception e) { throw new IllegalArgumentException("INVALID_DECISION", e); }
 }
 private static IllegalArgumentException invalid() { return new IllegalArgumentException("INVALID_DECISION"); }
 private static void text(ObjectNode n, String key, String pattern) {
  JsonNode v = n.get(key);
  if (v == null || !v.isTextual() || !v.textValue().matches(pattern)) throw invalid();
 }
 private static void number(ObjectNode n, String key) {
  JsonNode v = n.get(key);
  if (v == null || !v.isNumber() || !Double.isFinite(v.doubleValue()) || v.doubleValue() < 0) throw invalid();
 }
 public static String transactionId(ObjectNode n) { return n.get("transaction_id").textValue(); }
 public static String decisionId(ObjectNode n) { return n.get("decision_id").textValue(); }

 public static String hash(ObjectNode n) {
  try {
   byte[] canonical=Wire.JSON.writeValueAsBytes(sorted(n));
   return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(canonical));
  } catch (Exception e) { throw new IllegalArgumentException("INVALID_DECISION",e); }
 }
 private static JsonNode sorted(JsonNode n) {
  if(n.isObject()) {
   ObjectNode out=Wire.JSON.createObjectNode();
   java.util.TreeSet<String> keys=new java.util.TreeSet<>(); n.fieldNames().forEachRemaining(keys::add);
   for(String key:keys) out.set(key,sorted(n.get(key)));
   return out;
  }
  if(n.isArray()) {
   var out=Wire.JSON.createArrayNode(); for(JsonNode value:n) out.add(sorted(value)); return out;
  }
  return n;
 }
}
