package io.swipeline;

import org.junit.Test;
import static org.junit.Assert.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class CanonicalizerTest {
 static KeyedOneInputStreamOperatorTestHarness<String,DecisionRecord,byte[]> harness(Canonicalizer op) throws Exception {
  return new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(op),
   (DecisionRecord r)-> {try{return Decision.transactionId(Decision.parse(r.value()));}catch(IllegalArgumentException e){return "invalid";}},Types.STRING);
 }
 static DecisionRecord record(byte[] value) {return new DecisionRecord(value,2,17);}
 static String state(KeyedOneInputStreamOperatorTestHarness<String,DecisionRecord,byte[]> h) throws Exception {
  return ((org.apache.flink.streaming.api.operators.AbstractStreamOperator<byte[]>)h.getOperator()).getKeyedStateBackend().getPartitionedState(VoidNamespace.INSTANCE,VoidNamespaceSerializer.INSTANCE,
   new ValueStateDescriptor<String>("decision-hash",Types.STRING)).value();
 }
 @Test public void firstDecisionEmitsUnchangedAndStoresHash() throws Exception {
  var op=new Canonicalizer();
  try(var h=harness(op)) {
   h.open(); byte[] raw=DecisionTest.fixture("decision.json"); h.processElement(new StreamRecord<>(record(raw)));
   assertEquals(1,h.getOutput().size());
   byte[] actual=((StreamRecord<byte[]>)h.getOutput().poll()).getValue();
   assertEquals(Wire.JSON.readTree(raw),Wire.JSON.readTree(actual));
   assertEquals(Decision.hash(Decision.parse(raw)),state(h));
  }
 }
}
