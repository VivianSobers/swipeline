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
}
