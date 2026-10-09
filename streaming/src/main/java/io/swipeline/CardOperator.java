package io.swipeline;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.*;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
public class CardOperator extends KeyedProcessFunction<String,RequestRecord,RequestRecord> {
 public static final OutputTag<String> DLQ=new OutputTag<String>("auth-dlq"){};
 private transient ValueState<String> features;
 private transient MapState<String,String> seen;
 @Override public void open(OpenContext c)throws Exception{
  features=getRuntimeContext().getState(new ValueStateDescriptor<>("card-json-v1",Types.STRING));
  seen=getRuntimeContext().getMapState(new MapStateDescriptor<>("card-dedup-v1",Types.STRING,Types.STRING));
 }
 @Override public void processElement(RequestRecord record,Context ctx,Collector<RequestRecord> out)throws Exception{
  String value=record.value;long now=ctx.timerService().currentProcessingTime();ObjectNode n;
  try{n=Wire.parse(value);Wire.validate(n);}catch(Exception e){ctx.output(DLQ,record.quarantine("INVALID_REQUEST",now));return;}
  String id=n.path("event_id").asText(),cached=seen.get(id);
  if(cached!=null){
   if(!Wire.parse(cached).path("payload_hash").asText().equals(n.path("payload_hash").asText())){
    ctx.output(DLQ,record.quarantine("ID_CONFLICT",now));return;
   }
   out.collect(record.enriched(cached));return;
  }
  if(now<n.path("received_at_ms").asLong())getRuntimeContext().getMetricGroup().counter("clock_anomalies").inc();
  if(Rules.stale(n,now)){
   ObjectNode p=Rules.staleProposal(n,now);p.put("operational_stale",true);
   seen.put(id,p.toString());out.collect(record.enriched(p.toString()));return;
  }
  ObjectNode state=features.value()==null?Wire.empty():Wire.parse(features.value());
  try{n=Card.apply(state,n);}catch(ArithmeticException e){ctx.output(DLQ,record.quarantine("MONEY_OVERFLOW",now));return;}
  features.update(state.toString());seen.put(id,n.toString());out.collect(record.enriched(n.toString()));
 }
}
