package io.swipeline;
import java.math.BigInteger;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.*;
public final class Rules {
 public static ObjectNode score(ObjectNode input,long now) throws Exception {
  ObjectNode p=input.deepCopy();ArrayNode flags=p.putArray("fraud_flags");int score=0;
  long attempts=p.path("merchant_attempts").asLong(),small=p.path("merchant_small").asLong();
  boolean testing=attempts>0&&p.path("merchant_distinct_estimate").asDouble()>=50&&
   BigInteger.valueOf(small).multiply(BigInteger.valueOf(5)).compareTo(BigInteger.valueOf(attempts).multiply(BigInteger.valueOf(4)))>=0;
  if(testing){score+=100;flags.add("CARD_TESTING");}
  if(p.path("velocity_60s").asInt()>5){score+=40;flags.add("CARD_VELOCITY");}
  if(p.path("spend_anomaly").asBoolean()){score+=40;flags.add("SPEND_ANOMALY");}
  p.put("score",score);p.put("decision",score>=80?"DECLINE":"APPROVE");
  p.put("reason",flags.isEmpty()?"RULES_CLEAR":flags.get(0).asText());
  String id=Wire.id(p.path("transaction_id").asText(),"proposal");
  p.put("proposal_id",id);p.put("event_id",id);p.put("scored_at_ms",now);p.put("produced_at_ms",now);
  p.remove(java.util.List.of("spend_anomaly","merchant_attempts","merchant_small","spend_60s_minor"));
  return p;
 }
}
