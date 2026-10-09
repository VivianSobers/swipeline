package io.swipeline;
import org.junit.Test;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import static org.junit.Assert.*;
public class FeaturesTest {
 @Test public void javaSeventeen(){assertEquals("17",System.getProperty("java.specification.version"));}
@Test public void exactMoneyAndIds() throws Exception {
 assertEquals("825825018f58e795ae2d3d8df6a879567555c77654e9ca3071df2fe127aa9ebd",Wire.id("ab","c"));
 try {Wire.integer(Wire.parse("{\"amount_minor\":1.5}"),"amount_minor");fail();}catch(IllegalArgumentException expected){}
 try {Wire.integer(Wire.parse("{\"amount_minor\":9223372036854775808}"),"amount_minor");fail();}catch(IllegalArgumentException expected){}
}
}
