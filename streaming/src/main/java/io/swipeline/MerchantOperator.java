package io.swipeline;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
/** Updates merchant sketches once per transaction and emits its proposal; a replayed transaction gets its cached proposal. */
public class MerchantOperator extends KeyedProcessFunction<String,RequestRecord,String> {
 private transient ValueState<String> features;
 private transient MapState<String,String> seen;
 @Override public void open(OpenContext c)throws Exception{
  features=getRuntimeContext().getState(new ValueStateDescriptor<>("merchant-json-v1",Types.STRING));
  seen=getRuntimeContext().getMapState(new MapStateDescriptor<>("proposal-dedup-v1",Types.STRING,Types.STRING));
 }
 @Override public void processElement(RequestRecord record,Context ctx,Collector<String> out)throws Exception{
  long now=ctx.timerService().currentProcessingTime();
  ObjectNode n=Wire.parse(record.value);String id=n.path("transaction_id").asText(),cached=seen.get(id);
  if(cached!=null){
   if(!Wire.parse(cached).path("payload_hash").asText().equals(n.path("payload_hash").asText())){
    ctx.output(CardOperator.DLQ,record.quarantine("ID_CONFLICT",now));return;
   }
   out.collect(cached);return;
  }
  ObjectNode p;
  if(n.path("operational_stale").asBoolean()||Rules.stale(n,now)){
   p=Rules.staleProposal(n,now);p.remove("operational_stale");
  }else{
   ObjectNode state=features.value()==null?Wire.empty():Wire.parse(features.value());
   try{p=Rules.score(Merchant.apply(state,n),now);}catch(ArithmeticException e){ctx.output(CardOperator.DLQ,record.quarantine("MONEY_OVERFLOW",now));return;}
   features.update(state.toString());
  }
  seen.put(id,p.toString());out.collect(p.toString());
 }
}
