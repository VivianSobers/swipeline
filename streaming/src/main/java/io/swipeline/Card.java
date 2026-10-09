package io.swipeline;
import java.util.Iterator;
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
  n.put("usual_minor",3500);n.put("spend_ratio",(double)amount/3500);n.put("spend_anomaly",false);
  return n;
 }
}
