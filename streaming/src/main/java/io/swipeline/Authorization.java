package io.swipeline;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
/**
 * Authorization job: requests keyed by card, then by merchant, scored into
 * proposals that are written back immediately. Nothing on the reply path
 * waits for a checkpoint; state and source offsets checkpoint together.
 */
public final class Authorization {
 public static Properties consumer(){
  Properties p=new Properties();p.setProperty("fetch.min.bytes","1");p.setProperty("fetch.max.wait.ms","1");
  p.setProperty("max.poll.records","256");p.setProperty("isolation.level","read_uncommitted");p.setProperty("auto.offset.reset","none");return p;
 }
 public static Properties canonicalConsumer(){
  Properties p=consumer();p.setProperty("isolation.level","read_committed");return p;
 }
 public static Properties producer(){
  Properties p=new Properties();p.setProperty("linger.ms","0");p.setProperty("batch.size","32768");
  p.setProperty("acks","all");p.setProperty("enable.idempotence","true");p.setProperty("max.in.flight.requests.per.connection","5");return p;
 }
 /** Proposals are keyed by transaction ID, dead letters by payload hash. */
 static byte[] key(String v) throws Exception {
  var n=Wire.parse(v);return n.path("transaction_id").asText(n.path("payload_hash").asText()).getBytes(StandardCharsets.UTF_8);
 }
 static KafkaSink<String> sink(String brokers,String topic){
  return KafkaSink.<String>builder().setBootstrapServers(brokers).setKafkaProducerConfig(producer())
   .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
   .setRecordSerializer((KafkaRecordSerializationSchema<String>)(v,c,timestamp)->{
    try{return new ProducerRecord<>(topic,key(v),v.getBytes(StandardCharsets.UTF_8));}
    catch(Exception e){throw new IllegalArgumentException(e);}
   }).build();
 }
 /** Keeps each record's source partition and offset for dead letters. */
 static final class Requests implements KafkaRecordDeserializationSchema<RequestRecord> {
  @Override public void deserialize(ConsumerRecord<byte[],byte[]> r,Collector<RequestRecord> out){out.collect(new RequestRecord(r.value(),r.partition(),r.offset()));}
  @Override public TypeInformation<RequestRecord> getProducedType(){return TypeInformation.of(RequestRecord.class);}
 }
 static String key(RequestRecord r,String field){
  try{var n=Wire.parse(r.value);return Wire.JSON.writeValueAsString(new String[]{n.path("run_id").asText(),n.path(field).asText()});}
  catch(Exception e){return "invalid";}
 }
 static final class Decisions implements KafkaRecordDeserializationSchema<DecisionRecord> {
  @Override public void deserialize(ConsumerRecord<byte[],byte[]> r,Collector<DecisionRecord> out){out.collect(new DecisionRecord(r.value(),r.partition(),r.offset()));}
  @Override public TypeInformation<DecisionRecord> getProducedType(){return TypeInformation.of(DecisionRecord.class);}
 }
 public static StreamExecutionEnvironment build(Configuration cfg,String brokers,int parallelism){
  return build(cfg,brokers,parallelism,1);
 }
 public static StreamExecutionEnvironment build(Configuration cfg,String brokers,int parallelism,int canonParallelism){
  if(parallelism<1||parallelism>4)throw new IllegalArgumentException("parallelism 1..4");
  long bufferTimeout=Long.parseLong(cfg.toMap().getOrDefault("swipeline.authorization.buffer-timeout-ms","0"));
  if(bufferTimeout<0)throw new IllegalArgumentException("buffer timeout must be nonnegative");
  cfg.setString("execution.buffer-timeout.enabled","true");cfg.setString("execution.buffer-timeout.interval",bufferTimeout+" ms");
  cfg.setString("execution.checkpointing.interval","10 s");cfg.setString("execution.checkpointing.mode","EXACTLY_ONCE");
  cfg.setString("execution.checkpointing.timeout","60 s");
  // Standalone 2.2.1 exposes the restore path but no claim-mode CLI option.
  cfg.setString("execution.state-recovery.claim-mode","NO_CLAIM");
  cfg.setString("execution.checkpointing.externalized-checkpoint-retention","RETAIN_ON_CANCELLATION");
  cfg.setString("state.backend.type","rocksdb");cfg.setString("state.backend.incremental","true");
  var env=StreamExecutionEnvironment.getExecutionEnvironment(cfg);
  env.setParallelism(parallelism);env.setMaxParallelism(128);env.setBufferTimeout(bufferTimeout);
  // REST job configuration exposes the applied environment value, not just
  // the requested override. Graph tests also verify every operator's value.
  final long effectiveTimeout=env.getBufferTimeout();
  env.getConfig().setGlobalJobParameters(new org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters(){
   @Override public java.util.Map<String,String> toMap(){
    return java.util.Map.of("authorization.buffer-timeout-ms",Long.toString(effectiveTimeout));
   }
  });
  var source=KafkaSource.<RequestRecord>builder().setBootstrapServers(brokers).setTopics("swipeline.auth.requests.v1")
   .setGroupId("swipeline-authorization-v1").setProperties(consumer())
   .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
   .setDeserializer(new Requests()).build();
  var input=env.fromSource(source,WatermarkStrategy.noWatermarks(),"requests").uid("auth-source-v1");
  SingleOutputStreamOperator<RequestRecord> card=input.keyBy(r->key(r,"card_token"))
   .process(new CardOperator()).name("card").uid("card-v1");
  SingleOutputStreamOperator<String> merchant=card.keyBy(r->key(r,"merchant_id"))
   .process(new MerchantOperator()).name("merchant").uid("merchant-v1");
  merchant.sinkTo(sink(brokers,"swipeline.auth.proposals.v1")).uid("proposals-v1");
  card.getSideOutput(CardOperator.DLQ).union(merchant.getSideOutput(CardOperator.DLQ))
   .sinkTo(sink(brokers,"swipeline.dlq.v1")).uid("dlq-v1");
  var raw=KafkaSource.<DecisionRecord>builder().setBootstrapServers(brokers).setTopics("swipeline.decisions.raw.v1")
   .setGroupId("swipeline-canonical-v1").setProperties(canonicalConsumer())
   .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
   .setDeserializer(new Decisions()).build();
  var canonicalInput=env.fromSource(raw,WatermarkStrategy.noWatermarks(),"raw decisions").uid("canonical-source-v1").setParallelism(canonParallelism);
  env.addOperator(canonicalInput.getTransformation());
  return env;
 }
 public static void main(String[] args) throws Exception {
  var cfg=new Configuration();
  cfg.setString("swipeline.authorization.buffer-timeout-ms",System.getenv().getOrDefault("AUTH_BUFFER_TIMEOUT_MS","0"));
  build(cfg,System.getenv().getOrDefault("KAFKA_BROKERS","kafka:9092"),
   Integer.parseInt(System.getenv().getOrDefault("AUTH_PARALLELISM","2"))).execute("authorization");
 }
}
