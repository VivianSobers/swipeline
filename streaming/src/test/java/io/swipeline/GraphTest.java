package io.swipeline;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.HashSet;
import org.apache.flink.configuration.Configuration;
public class GraphTest {
 @Test public void effectiveReplyGraph() throws Exception {
  var env=Authorization.build(new Configuration(),"kafka:9092",2);
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
 @Test public void parallelismIsBounded() {
  assertThrows(IllegalArgumentException.class,()->Authorization.build(new Configuration(),"kafka:9092",5));
  assertThrows(IllegalArgumentException.class,()->Authorization.build(new Configuration(),"kafka:9092",0));
 }
 @Test public void recordsKeyByTransactionOrHash() throws Exception {
  assertEquals("tx",new String(Authorization.key("{\"transaction_id\":\"tx\",\"payload_hash\":\"h\"}"),java.nio.charset.StandardCharsets.UTF_8));
  assertEquals("h",new String(Authorization.key("{\"payload_hash\":\"h\"}"),java.nio.charset.StandardCharsets.UTF_8));
 }
}
