package io.swipeline;

import java.io.InputStream;
import org.junit.Test;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import static org.junit.Assert.*;

public class DecisionTest {
 @Test public void fixturesAreGatewayFinals() throws Exception {
  for (String name : new String[]{"decision.json", "decision-standin.json"}) {
   try (InputStream input = getClass().getResourceAsStream("/" + name)) {
    assertNotNull("missing gateway fixture " + name, input);
    JsonNode d = Wire.JSON.readTree(input);
    assertTrue(d.isObject());
    assertEquals(Wire.id(d.path("transaction_id").asText(), "decision"), d.path("decision_id").asText());
    assertEquals(name.equals("decision.json") ? "FLINK" : "STAND_IN", d.path("decision_source").asText());
    if (name.equals("decision-standin.json")) {
     for (String key : new String[]{"score", "velocity_60s", "usual_minor", "spend_ratio", "merchant_distinct_estimate", "features_late", "proposal_id"})
      assertTrue(key, d.has(key) && d.get(key).isNull());
     assertTrue(d.path("fraud_flags").isArray());
     assertEquals(0, d.path("fraud_flags").size());
    }
   }
  }
 }

 static byte[] fixture(String name) throws Exception {
  try (var input = DecisionTest.class.getResourceAsStream("/" + name)) { return input.readAllBytes(); }
 }
 static org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode fixture() throws Exception {
  return (org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode) Wire.JSON.readTree(fixture("decision.json"));
 }
 static void rejects(org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode n) throws Exception {
  assertThrows(IllegalArgumentException.class, () -> Decision.parse(Wire.JSON.writeValueAsBytes(n)));
 }
 @Test public void validatesRawDecisions() throws Exception {
  for (String name : new String[]{"decision.json", "decision-standin.json"})
   assertEquals(Wire.JSON.readTree(fixture(name)), Decision.parse(fixture(name)));
  assertThrows(IllegalArgumentException.class, () -> Decision.parse("[]".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
  var n=fixture(); n.remove("decision_id"); rejects(n);
  n=fixture(); n.put("decision_id", "0".repeat(64)); rejects(n);
  n=fixture(); n.put("decision", "MAYBE"); rejects(n);
  n=fixture(); n.put("schema_version", 2); rejects(n);
  n=(org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode) Wire.JSON.readTree(fixture("decision-standin.json"));
  n.put("score", 0); rejects(n);
  n=fixture(); n.putNull("score"); rejects(n);
  n=fixture(); n.putArray("fraud_flags").add(12); rejects(n);
  n=fixture(); n.put("fraud_flags", "CARD_TESTING"); rejects(n);
  assertThrows(IllegalArgumentException.class, () -> Decision.parse(new byte[65537]));
 }

 @Test public void canonicalHashSortsKeysAndDetectsEveryChange() throws Exception {
  var n=fixture();
  assertEquals("3569d2236369dad8c27fbe278ea3ff751cb02f9e9710d858683c96dc7b1db69b", Decision.hash(n));
  var reordered=Wire.JSON.createObjectNode();
  java.util.List<String> keys=new java.util.ArrayList<>(); n.fieldNames().forEachRemaining(keys::add);
  java.util.Collections.reverse(keys); for(String k:keys) reordered.set(k,n.get(k));
  assertEquals(Decision.hash(n),Decision.hash(reordered));
  var amount=n.deepCopy(); amount.put("amount_minor", n.get("amount_minor").longValue()+1);
  assertNotEquals(Decision.hash(n),Decision.hash(amount));
  var reason=n.deepCopy(); reason.put("reason","OTHER"); assertNotEquals(Decision.hash(n),Decision.hash(reason));
  var flags=n.deepCopy(); flags.withArray("fraud_flags").add("CARD_TESTING"); assertNotEquals(Decision.hash(n),Decision.hash(flags));
  assertEquals(Decision.hash(Wire.parse("{\"z\":[{\"b\":1,\"a\":\"<&>\"}],\"a\":0}")),
               Decision.hash(Wire.parse("{\"a\":0,\"z\":[{\"a\":\"<&>\",\"b\":1}]}")));
 }
}
