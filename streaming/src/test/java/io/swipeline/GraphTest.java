package io.swipeline;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.HashSet;
import org.apache.flink.configuration.Configuration;
public class GraphTest {
 @Test public void effectiveReplyGraph() throws Exception {
  var env=Authorization.build(new Configuration(),"kafka:9092",2);
  assertEquals("0",env.getConfig().getGlobalJobParameters().toMap().get("authorization.buffer-timeout-ms"));
  assertEquals(10000L,env.getCheckpointConfig().getCheckpointInterval());
  var graph=env.getStreamGraph();var uids=new HashSet<String>();
  for(var n:graph.getStreamNodes()){
   if(n.getTransformationUID()!=null)uids.add(n.getTransformationUID());
   assertEquals("buffer timeout of "+n.getOperatorName(),0,n.getBufferTimeout());
  }
  assertTrue(uids.toString(),uids.containsAll(java.util.List.of("auth-source-v1","card-v1","merchant-v1","proposals-v1","dlq-v1")));
  assertEquals(128,env.getMaxParallelism());assertEquals(2,env.getParallelism());
  assertEquals("1",Authorization.consumer().getProperty("fetch.max.wait.ms"));
  assertEquals("none",Authorization.consumer().getProperty("auto.offset.reset"));
  assertEquals("0",Authorization.producer().getProperty("linger.ms"));
  assertEquals("true",Authorization.producer().getProperty("enable.idempotence"));
  assertEquals("all",Authorization.producer().getProperty("acks"));
 }
 @Test public void explicitBufferTimeoutOverride() throws Exception {
  var cfg=new Configuration();
  cfg.setString("swipeline.authorization.buffer-timeout-ms","100");
  var env=Authorization.build(cfg,"kafka:9092",2);
  assertEquals("100",env.getConfig().getGlobalJobParameters().toMap().get("authorization.buffer-timeout-ms"));
  for(var n:env.getStreamGraph().getStreamNodes()){
   assertEquals("buffer timeout of "+n.getOperatorName(),100,n.getBufferTimeout());
  }
 }
 @Test public void restoredGraphDoesNotClaimCheckpoint() throws Exception {
  var cfg=new Configuration();
  cfg.setString("execution.state-recovery.path","file:///state/checkpoints/old/chk-1");
  cfg.setString("execution.state-recovery.claim-mode","CLAIM");
  var graph=Authorization.build(cfg,"kafka:9092",2).getStreamGraph().getJobGraph();
  assertEquals("file:///state/checkpoints/old/chk-1",graph.getSavepointRestoreSettings().getRestorePath());
  assertEquals(org.apache.flink.core.execution.RecoveryClaimMode.NO_CLAIM,graph.getSavepointRestoreSettings().getRecoveryClaimMode());
 }
 @Test public void parallelismIsBounded() {
  assertThrows(IllegalArgumentException.class,()->Authorization.build(new Configuration(),"kafka:9092",5));
  assertThrows(IllegalArgumentException.class,()->Authorization.build(new Configuration(),"kafka:9092",0));
 }
 @Test public void recordsKeyByTransactionOrHash() throws Exception {
  assertEquals("tx",new String(Authorization.key("{\"transaction_id\":\"tx\",\"payload_hash\":\"h\"}"),java.nio.charset.StandardCharsets.UTF_8));
  assertEquals("h",new String(Authorization.key("{\"payload_hash\":\"h\"}"),java.nio.charset.StandardCharsets.UTF_8));
 }

 static Object field(Object object,String name) throws Exception {
  var f=object.getClass().getDeclaredField(name);f.setAccessible(true);return f.get(object);
 }
 static org.apache.flink.streaming.api.graph.StreamNode node(org.apache.flink.streaming.api.graph.StreamGraph graph,String uid) {
  return graph.getStreamNodes().stream().filter(n->uid.equals(n.getTransformationUID())).findFirst().orElseThrow(()->new AssertionError("missing "+uid));
 }
 @Test public void canonicalSourceReadsCommittedWithCoordinates() throws Exception {
  var graph=Authorization.build(new Configuration(),"kafka:9092",2,3).getStreamGraph();
  var n=node(graph,"canonical-source-v1");assertEquals(3,n.getParallelism());
  Object source=field(n.getOperatorFactory(),"source");
  var props=(java.util.Properties)field(source,"props");
  assertEquals("swipeline-canonical-v1",props.getProperty("group.id"));
  assertEquals("read_committed",props.getProperty("isolation.level"));
  assertEquals("1",props.getProperty("fetch.min.bytes"));assertEquals("1",props.getProperty("fetch.max.wait.ms"));
  assertEquals("256",props.getProperty("max.poll.records"));
  assertEquals("none",Authorization.canonicalConsumer().getProperty("auto.offset.reset"));
  var offsets=(org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer)field(source,"startingOffsetsInitializer");
  assertEquals(org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST,offsets.getAutoOffsetResetStrategy());
  var tp=new org.apache.kafka.common.TopicPartition("swipeline.decisions.raw.v1",2);
  assertEquals(Long.valueOf(org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit.COMMITTED_OFFSET),offsets.getPartitionOffsets(java.util.Set.of(tp),null).get(tp));
  var deserializer=(org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema<DecisionRecord>)field(source,"deserializationSchema");
  var records=new java.util.ArrayList<DecisionRecord>();byte[] raw=DecisionTest.fixture("decision.json");
  deserializer.deserialize(new org.apache.kafka.clients.consumer.ConsumerRecord<byte[],byte[]>(tp.topic(),2,123,null,raw),new org.apache.flink.util.Collector<DecisionRecord>() {
   public void collect(DecisionRecord record){records.add(record);}public void close(){}
  });
  assertEquals(1,records.size());assertArrayEquals(raw,records.get(0).value());assertEquals(2,records.get(0).partition());assertEquals(123,records.get(0).offset());
  var serializer=(org.apache.flink.api.common.typeutils.TypeSerializer<DecisionRecord>)n.getTypeSerializerOut();
  var bytes=new org.apache.flink.core.memory.DataOutputSerializer(1024);serializer.serialize(records.get(0),bytes);
  var copy=serializer.deserialize(new org.apache.flink.core.memory.DataInputDeserializer(bytes.getCopyOfBuffer()));
  assertArrayEquals(raw,copy.value());assertEquals(2,copy.partition());assertEquals(123,copy.offset());
 }

 @Test public void canonicalizerKeysByTransactionAndBoundsParallelism() throws Exception {
  var graph=Authorization.build(new Configuration(),"kafka:9092",2,3).getStreamGraph();
  var n=node(graph,"canonical-v1");assertEquals(3,n.getParallelism());
  assertEquals(node(graph,"canonical-source-v1").getId(),n.getInEdges().get(0).getSourceId());
  assertTrue(n.getInEdges().get(0).getPartitioner() instanceof org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner);
  var selector=(org.apache.flink.api.java.functions.KeySelector<DecisionRecord,String>)n.getStatePartitioners()[0];
  var raw=DecisionTest.fixture("decision.json");
  assertEquals(Decision.transactionId(Decision.parse(raw)),selector.getKey(new DecisionRecord(raw,2,17)));
  assertEquals("invalid",selector.getKey(new DecisionRecord(new byte[]{0},2,18)));
  assertThrows(IllegalArgumentException.class,()->Authorization.build(new Configuration(),"kafka:9092",2,0));
  assertThrows(IllegalArgumentException.class,()->Authorization.build(new Configuration(),"kafka:9092",2,5));
 }

 @Test public void canonicalSinkIsTransactionalAndKeysUnchangedFinals() throws Exception {
  var graph=Authorization.build(new Configuration(),"kafka:9092",2,3).getStreamGraph();
  var n=node(graph,"canonical-sink-v1");assertEquals(3,n.getParallelism());
  var sink=field(n.getOperatorFactory(),"sink");
  assertEquals(org.apache.flink.connector.base.DeliveryGuarantee.EXACTLY_ONCE,field(sink,"deliveryGuarantee"));
  assertEquals("swipeline-canonical-v1",field(sink,"transactionalIdPrefix"));
  assertEquals("600000",((java.util.Properties)field(sink,"kafkaProducerConfig")).getProperty("transaction.timeout.ms"));
  var serializer=(org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema<byte[]>)field(sink,"recordSerializer");
  byte[] raw=DecisionTest.fixture("decision.json");var record=serializer.serialize(raw,null,null);
  assertEquals("swipeline.decisions.v1",record.topic());assertArrayEquals(raw,record.value());
  assertEquals(Decision.transactionId(Decision.parse(raw)),new String(record.key(),java.nio.charset.StandardCharsets.UTF_8));
  var dlq=node(graph,"canonical-dlq-v1");assertEquals(3,dlq.getParallelism());
  assertEquals(Canonicalizer.DLQ,dlq.getInEdges().get(0).getOutputTag());
  assertEquals(org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE,field(field(dlq.getOperatorFactory(),"sink"),"deliveryGuarantee"));
 }

 @Test public void canonicalEnvironmentDefaultsAndOverrides() throws Exception {
  assertEquals(1,node(Authorization.environment(java.util.Map.of()).getStreamGraph(),"canonical-v1").getParallelism());
  var graph=Authorization.environment(java.util.Map.of("CANON_PARALLELISM","4","AUTH_PARALLELISM","2")).getStreamGraph();
  for(String uid:new String[]{"canonical-source-v1","canonical-v1","canonical-sink-v1","canonical-dlq-v1"})assertEquals(4,node(graph,uid).getParallelism());
  assertEquals(2,node(graph,"card-v1").getParallelism());
 }
}
