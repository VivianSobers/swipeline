package io.swipeline;
import org.junit.Test;
import static org.junit.Assert.*;
import java.nio.charset.StandardCharsets;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
public class OperatorsTest {
 static String fixture() throws Exception {
  try(var in=OperatorsTest.class.getResourceAsStream("/request.json")){return new String(in.readAllBytes(),StandardCharsets.UTF_8);}
 }
 static RequestRecord record(String value){return new RequestRecord(value.getBytes(StandardCharsets.UTF_8),2,17);}
 static ObjectNode changed(String tx,long amount,long at) throws Exception {
  ObjectNode n=Wire.parse(fixture());n.put("transaction_id",Wire.id(tx));n.put("amount_minor",amount);n.put("event_time_ms",at);
  n.put("event_id",Wire.id(n.path("transaction_id").asText(),"authorization","1"));
  ObjectNode b=Wire.JSON.createObjectNode();
  for(String k:new String[]{"run_id","transaction_id","source_seq","event_time_ms","card_token","merchant_id","mcc","issuer_bank_id","acquirer_bank_id","currency","amount_minor","authorization_date","rule_version","reference_version"})b.set(k,n.get(k));
  n.put("payload_hash",java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(b.toString().getBytes(StandardCharsets.UTF_8))));
  Wire.validate(n);return n;
 }
 static KeyedOneInputStreamOperatorTestHarness<String,RequestRecord,RequestRecord> card() throws Exception {
  return new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(new CardOperator()),(RequestRecord r)->"card",Types.STRING);
 }
 static RequestRecord cardOutput(KeyedOneInputStreamOperatorTestHarness<String,RequestRecord,RequestRecord> h){return ((StreamRecord<RequestRecord>)h.getOutput().poll()).getValue();}
 @Test public void malformedRequestsGoToDeadLetters() throws Exception {
  try(var h=card()){
   h.open();h.setProcessingTime(1000000);h.processElement(new StreamRecord<>(record("{\"schema_version\":2}")));
   assertEquals(0,h.getOutput().size());assertEquals(1,h.getSideOutput(CardOperator.DLQ).size());
   ObjectNode q=Wire.parse(h.getSideOutput(CardOperator.DLQ).peek().getValue());
   assertEquals(2,q.path("partition").asInt());assertEquals(17,q.path("offset").asLong());
   h.processElement(new StreamRecord<>(record(fixture())));assertEquals(1,Wire.parse(cardOutput(h).value).path("velocity_60s").asInt());
  }
 }
 @Test public void cardDuplicateAndFeaturesSurviveRestore() throws Exception {
  OperatorSubtaskState snapshot;String first;
  try(var h=card()){
   h.open();h.setProcessingTime(1000000);h.processElement(new StreamRecord<>(record(fixture())));
   first=cardOutput(h).value;snapshot=h.snapshot(1,1000000);
  }
  try(var h=card()){
   h.initializeState(snapshot);h.open();h.setProcessingTime(1005000);
   h.processElement(new StreamRecord<>(record(fixture())));assertEquals(first,cardOutput(h).value);
   h.setProcessingTime(1005001);ObjectNode next=changed("next",100,1791417600001L);next.put("received_at_ms",1005000);
   h.processElement(new StreamRecord<>(record(next.toString())));
   assertEquals(2,Wire.parse(cardOutput(h).value).path("velocity_60s").asInt());
  }
 }
 @Test public void staleAndConflictingRequestsDoNotUpdateCard() throws Exception {
  try(var h=card()){
   h.open();h.setProcessingTime(1002001);h.processElement(new StreamRecord<>(record(fixture())));
   ObjectNode stale=Wire.parse(cardOutput(h).value);assertEquals("STALE_REQUEST",stale.path("reason").asText());
   ObjectNode next=changed("next",100,1791417600001L);next.put("received_at_ms",1002001);
   h.processElement(new StreamRecord<>(record(next.toString())));assertEquals(1,Wire.parse(cardOutput(h).value).path("velocity_60s").asInt());
   ObjectNode conflict=changed("next",200,1791417600001L);conflict.put("received_at_ms",1002001);
   h.processElement(new StreamRecord<>(record(conflict.toString())));
   assertTrue(h.getOutput().isEmpty());assertEquals("ID_CONFLICT",Wire.parse(h.getSideOutput(CardOperator.DLQ).peek().getValue()).path("error_code").asText());
   ObjectNode last=changed("last",100,1791417600002L);last.put("received_at_ms",1002001);
   h.processElement(new StreamRecord<>(record(last.toString())));assertEquals(2,Wire.parse(cardOutput(h).value).path("velocity_60s").asInt());
  }
 }
 @Test public void overflowDoesNotPersistPartialCardState() throws Exception {
  try(var h=card()){
   h.open();h.setProcessingTime(1000000);
   h.processElement(new StreamRecord<>(record(changed("big",Long.MAX_VALUE,1791417600000L).toString())));cardOutput(h);
   h.processElement(new StreamRecord<>(record(changed("overflow",1,1791417600001L).toString())));
   assertTrue(h.getOutput().isEmpty());assertEquals("MONEY_OVERFLOW",Wire.parse(h.getSideOutput(CardOperator.DLQ).peek().getValue()).path("error_code").asText());
   h.processElement(new StreamRecord<>(record(changed("later",1,1791417661000L).toString())));
   assertEquals(1,Wire.parse(cardOutput(h).value).path("velocity_60s").asInt());
  }
 }
 static KeyedOneInputStreamOperatorTestHarness<String,RequestRecord,String> merchant() throws Exception {
  return new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(new MerchantOperator()),(RequestRecord r)->"merchant",Types.STRING);
 }
 static String merchantOutput(KeyedOneInputStreamOperatorTestHarness<String,RequestRecord,String> h){return ((StreamRecord<String>)h.getOutput().poll()).getValue();}
 static ObjectNode cardOf(String tx,String card,long amount,long at) throws Exception {
  ObjectNode n=Wire.parse(fixture());n.put("card_token",card);n.put("transaction_id",Wire.id(tx));n.put("amount_minor",amount);n.put("event_time_ms",at);
  n.put("event_id",Wire.id(n.path("transaction_id").asText(),"authorization","1"));
  ObjectNode b=Wire.JSON.createObjectNode();
  for(String k:new String[]{"run_id","transaction_id","source_seq","event_time_ms","card_token","merchant_id","mcc","issuer_bank_id","acquirer_bank_id","currency","amount_minor","authorization_date","rule_version","reference_version"})b.set(k,n.get(k));
  n.put("payload_hash",java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(b.toString().getBytes(StandardCharsets.UTF_8))));
  Wire.validate(n);return n;
 }
 /** The proposal for the gateway's own fixture is pinned in proposal.json, which the Go validator also checks. */
 @Test public void fixtureProposalMatchesGatewayContract() throws Exception {
  String enriched;
  try(var h=card()){h.open();h.setProcessingTime(1000010);h.processElement(new StreamRecord<>(record(fixture())));enriched=cardOutput(h).value;}
  try(var h=merchant()){
   h.open();h.setProcessingTime(1000020);h.processElement(new StreamRecord<>(record(fixture()).enriched(enriched)));
   ObjectNode p=Wire.parse(merchantOutput(h));
   String golden;try(var in=OperatorsTest.class.getResourceAsStream("/proposal.json")){golden=new String(in.readAllBytes(),StandardCharsets.UTF_8);}
   assertEquals(Wire.parse(golden),p);
   ObjectNode request=Wire.parse(fixture());
   for(String k:new String[]{"transaction_id","payload_hash","rule_version","run_id","event_time_ms","source_seq"})assertEquals(request.get(k),p.get(k));
   assertEquals(Wire.id(request.path("transaction_id").asText(),"proposal"),p.path("proposal_id").asText());
   assertTrue(p.path("usual_minor").asLong()>=100);assertEquals(1000020,p.path("scored_at_ms").asLong());
  }
 }
 @Test public void merchantDuplicateSurvivesRestore() throws Exception {
  String enriched;
  try(var h=card()){h.open();h.setProcessingTime(1000010);h.processElement(new StreamRecord<>(record(fixture())));enriched=cardOutput(h).value;}
  OperatorSubtaskState snapshot;String first;
  try(var h=merchant()){
   h.open();h.setProcessingTime(1000020);h.processElement(new StreamRecord<>(record(fixture()).enriched(enriched)));
   first=merchantOutput(h);snapshot=h.snapshot(1,1000020);
  }
  try(var h=merchant()){
   h.initializeState(snapshot);h.open();h.setProcessingTime(1000030);
   h.processElement(new StreamRecord<>(record(fixture()).enriched(enriched)));
   assertEquals(first,merchantOutput(h));
   ObjectNode other=cardOf("other","card_"+"1".repeat(64),100,1791417600001L);
   try(var c=card()){c.open();c.setProcessingTime(1000030);c.processElement(new StreamRecord<>(record(other.toString())));
    h.processElement(new StreamRecord<>(record(other.toString()).enriched(cardOutput(c).value)));}
   assertEquals(2,Wire.parse(merchantOutput(h)).path("merchant_distinct_estimate").asDouble(),0.1);
  }
 }
 @Test public void cardTestingBurstIsDeclined() throws Exception {
  try(var c=card();var h=merchant()){
   c.open();h.open();c.setProcessingTime(1000010);h.setProcessingTime(1000020);ObjectNode last=null;
   for(int i=0;i<80;i++){
    ObjectNode n=cardOf("burst-"+i,String.format("card_%064x",i),100,1791417600000L+i);
    c.processElement(new StreamRecord<>(record(n.toString())));
    h.processElement(new StreamRecord<>(record(n.toString()).enriched(cardOutput(c).value)));
    last=Wire.parse(merchantOutput(h));
    if(i==10)assertEquals("APPROVE",last.path("decision").asText());
   }
   assertEquals("DECLINE",last.path("decision").asText());assertEquals("CARD_TESTING",last.path("reason").asText());
   assertEquals("CARD_TESTING",last.path("fraud_flags").get(0).asText());
  }
 }
}
