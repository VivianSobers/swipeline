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
}
