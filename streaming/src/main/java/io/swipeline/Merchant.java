package io.swipeline;
import java.util.Iterator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.*;
public final class Merchant {
 public static ObjectNode apply(ObjectNode s,ObjectNode input) throws Exception {
  ObjectNode n=input.deepCopy();long at=Wire.integer(n,"event_time_ms");
  long previous=s.path("frontier").asLong(at),frontier=Math.max(at,previous),slot=frontier/10000;
  s.put("frontier",frontier);n.put("features_late",n.path("features_late").asBoolean()||at<previous);
  ObjectNode buckets=s.has("buckets")?(ObjectNode)s.get("buckets"):s.putObject("buckets");
  for(Iterator<String> it=buckets.fieldNames();it.hasNext();)if(Long.parseLong(it.next())<=slot-6)it.remove();
  if(at/10000>slot-6){
   String key=Long.toString(at/10000);ObjectNode b=buckets.has(key)?(ObjectNode)buckets.get(key):buckets.putObject(key);
   byte[] h=b.has("hll")?b.get("hll").binaryValue():new byte[4096];Hll.add(h,n.path("card_token").asText());b.put("hll",h);
   b.put("attempts",Math.addExact(b.path("attempts").asLong(),1));
   b.put("small",Math.addExact(b.path("small").asLong(),Wire.integer(n,"amount_minor")<=200?1:0));
  }
  byte[] union=new byte[4096];long attempts=0,small=0;
  for(JsonNode b:buckets){
   byte[] h=b.get("hll").binaryValue();for(int i=0;i<4096;i++)union[i]=(byte)Math.max(union[i]&255,h[i]&255);
   attempts=Math.addExact(attempts,b.path("attempts").asLong());small=Math.addExact(small,b.path("small").asLong());
  }
  n.put("merchant_distinct_estimate",Hll.estimate(union));n.put("merchant_attempts",attempts);n.put("merchant_small",small);
  return n;
 }
}
