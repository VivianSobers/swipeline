package io.swipeline;
import org.junit.Test;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import static org.junit.Assert.*;
public class FeaturesTest {
 @Test public void javaSeventeen(){assertEquals("17",System.getProperty("java.specification.version"));}
@Test public void exactMoneyAndIds() throws Exception {
 assertEquals("825825018f58e795ae2d3d8df6a879567555c77654e9ca3071df2fe127aa9ebd",Wire.id("ab","c"));
 try {Wire.integer(Wire.parse("{\"amount_minor\":1.5}"),"amount_minor");fail();}catch(IllegalArgumentException expected){}
 try {Wire.integer(Wire.parse("{\"amount_minor\":9223372036854775808}"),"amount_minor");fail();}catch(IllegalArgumentException expected){}
}
private ObjectNode request(long at,long amount) throws Exception {
 ObjectNode n=Wire.parse("{\"mcc\":\"5411\",\"card_token\":\"card_fixture\",\"merchant_id\":\"merchant-1\"}");
 n.put("event_time_ms",at);n.put("amount_minor",amount);return n;
}
@Test public void cardBucketsExpire() throws Exception {
 ObjectNode s=Wire.empty();
 for(int i=0;i<6;i++)assertEquals(i+1,Card.apply(s,request(1000+i,100)).path("velocity_60s").asInt());
 assertEquals(1,Card.apply(s,request(62000,100)).path("velocity_60s").asInt());
 ObjectNode late=Card.apply(s,request(1000,100));
 assertTrue(late.path("features_late").asBoolean());assertEquals(1,late.path("velocity_60s").asInt());
}

@Test public void cardWindowIncludesCurrentAndActiveLateBuckets() throws Exception {
 ObjectNode s=Wire.empty();
 Card.apply(s,request(1000,100));
 ObjectNode n=Card.apply(s,request(60000,200));
 assertEquals(2,n.path("velocity_60s").asInt());assertEquals(300,n.path("spend_60s_minor").asLong());
 n=Card.apply(s,request(59000,300));assertTrue(n.path("features_late").asBoolean());assertEquals(600,n.path("spend_60s_minor").asLong());
 n=Card.apply(s,request(61000,400));assertEquals(3,n.path("velocity_60s").asInt());assertEquals(900,n.path("spend_60s_minor").asLong());
 for(int i=62;i<200;i++)Card.apply(s,request(i*1000L,1));
 assertEquals(60,s.path("buckets").size());
}
@Test public void baselineExcludesCurrentAndPoison() throws Exception {
 ObjectNode s=Wire.empty();
 for(int i=0;i<10;i++)Card.apply(s,request(i*61000L,1000));
 ObjectNode n=Card.apply(s,request(610000,50000));
 assertEquals(1000,n.path("usual_minor").asLong());assertTrue(n.path("spend_anomaly").asBoolean());
 assertEquals(10,s.path("history").size());
}
@Test public void exactOverflowIsRejected() throws Exception {
 ObjectNode s=Wire.empty();Card.apply(s,request(1000,Long.MAX_VALUE));
 try {Card.apply(s,request(1001,1));fail();}catch(ArithmeticException expected){}
}

@Test public void baselineDefaultsBoundsAndVelocityEligibility() throws Exception {
 for(String mcc:new String[]{"5411","5812","5541","5732"}) {
  ObjectNode n=request(0,100);n.put("mcc",mcc);
  long expected=switch(mcc){case "5812"->2200;case "5541"->6000;case "5732"->40000;default->3500;};
  assertEquals(expected,Card.apply(Wire.empty(),n).path("usual_minor").asLong());
 }
 ObjectNode s=Wire.empty();for(int i=0;i<110;i++)Card.apply(s,request(i*61000L,i<10?1000:2000));
 assertEquals(100,s.path("history").size());
 assertEquals(2000,Card.apply(s,request(110*61000L,2000)).path("usual_minor").asLong());
 s=Wire.empty();for(int i=0;i<6;i++)Card.apply(s,request(i,100));assertEquals(5,s.path("history").size());
 s=Wire.empty();for(int i=0;i<10;i++)Card.apply(s,request(i*61000L,1));
 assertEquals(100,Card.apply(s,request(610000,10000)).path("usual_minor").asLong());
 assertFalse(Card.apply(s,request(671000,10000)).path("spend_anomaly").asBoolean());
}
@Test public void hllFixedSeedTolerance() throws Exception {
 for(int count:new int[]{80,500,2000,10000}) {
  byte[] h=new byte[4096];for(int i=0;i<count;i++)Hll.add(h,"card_"+i);
  assertEquals(count,Hll.estimate(h),count*0.06);
  byte[] before=h.clone();for(int i=0;i<count;i++)Hll.add(h,"card_"+i);
  assertArrayEquals(before,h);
 }
}
@Test public void merchantUnionAndExpiry() throws Exception {
 ObjectNode s=Wire.empty(),n=null;
 for(int i=0;i<80;i++){n=request(10000+i,100);n.put("card_token","card_"+i);n=Merchant.apply(s,n);}
 assertTrue(n.path("merchant_distinct_estimate").asDouble()>65);
 n=Merchant.apply(s,request(80000,100));assertEquals(1,n.path("merchant_distinct_estimate").asDouble(),0.1);
 assertTrue(s.path("buckets").size()<=6);
}

@Test public void merchantUnionDeduplicatesAcrossBucketsAndHandlesLateness() throws Exception {
 ObjectNode s=Wire.empty(),n=null;
 for(int slot=0;slot<6;slot++)for(int i=0;i<80;i++) {
  n=request(slot*10000L, i<64?200:201);n.put("card_token","card_"+i);n=Merchant.apply(s,n);
 }
 assertEquals(80,n.path("merchant_distinct_estimate").asDouble(),4.8);
 assertEquals(480,n.path("merchant_attempts").asLong());assertEquals(384,n.path("merchant_small").asLong());
 n=Merchant.apply(s,request(60000,200));assertEquals(401,n.path("merchant_attempts").asLong());assertEquals(6,s.path("buckets").size());
 n=Merchant.apply(s,request(0,200));assertTrue(n.path("features_late").asBoolean());assertEquals(401,n.path("merchant_attempts").asLong());
 n=Merchant.apply(s,request(10000,200));assertEquals(402,n.path("merchant_attempts").asLong());
 ObjectNode restored=Wire.parse(s.toString());
 assertEquals(Merchant.apply(s,request(70000,200)),Merchant.apply(restored,request(70000,200)));
}
@Test public void ruleThresholdAndPrecedence() throws Exception {
 ObjectNode n=request(1000,100);n.put("transaction_id","tx");n.put("velocity_60s",6);n.put("spend_anomaly",true);
 n.put("merchant_distinct_estimate",80);n.put("merchant_attempts",100);n.put("merchant_small",80);
 ObjectNode p=Rules.score(n,1000);
 assertEquals("DECLINE",p.path("decision").asText());assertEquals("CARD_TESTING",p.path("reason").asText());assertEquals(3,p.path("fraud_flags").size());
 n.put("merchant_distinct_estimate",0);n.put("spend_anomaly",false);
 assertEquals("APPROVE",Rules.score(n,1000).path("decision").asText());
}

@Test public void rulesRespectExactThresholdsAndDoNotMutateInput() throws Exception {
 ObjectNode n=request(1000,100);n.put("transaction_id","tx");n.put("velocity_60s",5);
 ObjectNode before=n.deepCopy(),p=Rules.score(n,1000);assertEquals(before,n);assertEquals(0,p.path("score").asInt());assertEquals("RULES_CLEAR",p.path("reason").asText());
 n.put("velocity_60s",6);n.put("spend_anomaly",true);p=Rules.score(n,1000);
 assertEquals(80,p.path("score").asInt());assertEquals("DECLINE",p.path("decision").asText());assertEquals("CARD_VELOCITY",p.path("reason").asText());
 n.put("velocity_60s",0);n.put("spend_anomaly",false);n.put("merchant_distinct_estimate",50);n.put("merchant_attempts",100);n.put("merchant_small",79);
 assertEquals("APPROVE",Rules.score(n,1000).path("decision").asText());
 n.put("merchant_small",80);assertEquals(100,Rules.score(n,1000).path("score").asInt());
 n.put("merchant_distinct_estimate",49.99);assertEquals(0,Rules.score(n,1000).path("score").asInt());
}
@Test public void stalenessUsesOnlyAdmissionLag() throws Exception {
 ObjectNode n=request(1,100);n.put("received_at_ms",10000);n.put("transaction_id","tx");
 assertFalse(Rules.stale(n,12000));assertTrue(Rules.stale(n,12001));assertFalse(Rules.stale(n,9000));
 assertEquals("STALE_REQUEST",Rules.staleProposal(n,12001).path("reason").asText());
}

@Test public void staleProposalDoesNotMutateInput() throws Exception {
 ObjectNode n=request(1,100);n.put("received_at_ms",10000);n.put("transaction_id","tx");
 ObjectNode before=n.deepCopy(),p=Rules.staleProposal(n,12001);
 assertEquals(before,n);assertEquals("DECLINE",p.path("decision").asText());assertEquals(0,p.path("fraud_flags").size());
 n.put("event_time_ms",Long.MAX_VALUE);assertTrue(Rules.stale(n,12001));
}
@Test public void staleProposalClearsPriorFraudSignals() throws Exception {
 ObjectNode n=request(1,50000);n.put("received_at_ms",10000);n.put("transaction_id","tx");
 n.put("spend_anomaly",true);n.put("velocity_60s",6);n.put("merchant_distinct_estimate",80);
 n.put("merchant_attempts",100);n.put("merchant_small",80);
 ObjectNode before=n.deepCopy(),p=Rules.staleProposal(n,12001);
 assertEquals(0,p.path("score").asInt());assertEquals(0,p.path("fraud_flags").size());
 assertEquals("STALE_REQUEST",p.path("reason").asText());assertEquals("DECLINE",p.path("decision").asText());assertEquals(before,n);
}
@Test public void gatewayIdGoldenValues() throws Exception {
 assertEquals("52906ab0686d6fb4f6f95a89b153c9311607aa6cd4ad335195bc0d9e784862d0",Wire.id("tx","proposal"));
 assertEquals("cbfd8f6a0cc82d1008e03fd1bf6cbb8c336a5c476143c86bd6619aa737cee122",Wire.id("a<b&c>","decision"));
}
@Test public void admittedRequestValidatesUnchanged() throws Exception {
 ObjectNode n=Wire.parse(new String(getClass().getResourceAsStream("/request.json").readAllBytes(),java.nio.charset.StandardCharsets.UTF_8));
 ObjectNode before=n.deepCopy();Wire.validate(n);assertEquals(before,n);
 ObjectNode changed=n.deepCopy();changed.put("amount_minor",1956);assertThrows(IllegalArgumentException.class,()->Wire.validate(changed));
 ObjectNode wrongId=n.deepCopy();wrongId.put("event_id","wrong");assertThrows(IllegalArgumentException.class,()->Wire.validate(wrongId));
 ObjectNode missing=n.deepCopy();missing.remove("received_at_ms");assertThrows(IllegalArgumentException.class,()->Wire.validate(missing));
 assertThrows(IllegalArgumentException.class,()->Wire.validate(Wire.parse("{\"schema_version\":2}")));
}
@Test public void quarantinePreservesSourceAndRedactsRawInput() throws Exception {
 byte[] raw=new byte[]{(byte)0xff,0,38};
 ObjectNode q=Wire.parse(Wire.quarantine(raw,2,17,"INVALID_REQUEST",1000));
 assertEquals("swipeline.auth.requests.v1",q.path("origin_topic").asText());
 assertEquals(2,Wire.integer(q,"partition"));assertEquals(17,Wire.integer(q,"offset"));
 assertTrue(q.path("error_code").asText().matches("^[A-Z][A-Z_]{0,63}$"));
 assertTrue(q.path("payload_hash").asText().matches("^[0-9a-f]{64}$"));
 assertEquals(java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(raw)),q.path("payload_hash").asText());
 assertEquals("redacted",q.path("sanitized_input").asText());assertEquals(1000,Wire.integer(q,"detected_at_ms"));
}
}
