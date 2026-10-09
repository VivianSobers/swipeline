package io.swipeline;
import java.util.Iterator;
import java.math.BigInteger;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.*;
public final class Card {
 public static ObjectNode apply(ObjectNode s,ObjectNode input) {
  ObjectNode n=input.deepCopy();long at=Wire.integer(n,"event_time_ms"),amount=Wire.integer(n,"amount_minor");
  long previous=s.path("frontier").asLong(at),frontier=Math.max(at,previous),second=frontier/1000;
  n.put("features_late",at<previous);s.put("frontier",frontier);
  ObjectNode buckets=s.has("buckets")?(ObjectNode)s.get("buckets"):s.putObject("buckets");
  for(Iterator<String> it=buckets.fieldNames();it.hasNext();)if(Long.parseLong(it.next())<=second-60)it.remove();
  boolean active=at/1000>second-60;
  if(active){
   String key=Long.toString(at/1000);
   ObjectNode b=buckets.has(key)?(ObjectNode)buckets.get(key):buckets.putObject(key);
   b.put("count",Math.addExact(b.path("count").asLong(),1));
   b.put("spend",Math.addExact(b.path("spend").asLong(),amount));
  }
  long count=0,spend=0;
  for(JsonNode b:buckets){count=Math.addExact(count,b.path("count").asLong());spend=Math.addExact(spend,b.path("spend").asLong());}
  n.put("velocity_60s",Math.toIntExact(count));n.put("spend_60s_minor",spend);
ArrayNode history=s.has("history")?(ArrayNode)s.get("history"):s.putArray("history");
long usual=switch(n.path("mcc").asText()){case "5812"->2200;case "5541"->6000;case "5732"->40000;default->3500;};
if(history.size()>=10){
 BigInteger sum=BigInteger.ZERO;for(JsonNode h:history)sum=sum.add(BigInteger.valueOf(h.longValue()));
 usual=sum.divide(BigInteger.valueOf(history.size())).longValueExact();
}
usual=Math.max(100,usual);
boolean anomaly=history.size()>=10&&amount>10000&&BigInteger.valueOf(amount).compareTo(BigInteger.valueOf(usual).multiply(BigInteger.valueOf(4)))>0;
n.put("usual_minor",usual);n.put("spend_ratio",(double)amount/usual);n.put("spend_anomaly",anomaly);
if(active&&count<=5&&!anomaly){history.add(amount);if(history.size()>100)history.remove(0);}
  return n;
 }
}
