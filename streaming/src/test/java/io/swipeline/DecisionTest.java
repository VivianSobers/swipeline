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
}
