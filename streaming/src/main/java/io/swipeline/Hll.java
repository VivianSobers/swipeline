package io.swipeline;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
public final class Hll {
 public static void add(byte[] r,String card) throws Exception {
  byte[] digest=MessageDigest.getInstance("SHA-256").digest(("swipeline-hll-v1:"+card).getBytes(StandardCharsets.UTF_8));
  long h=ByteBuffer.wrap(digest).getLong();int index=(int)(h>>>52);
  int rank=Math.min(53,Long.numberOfLeadingZeros(h<<12)+1);
  r[index]=(byte)Math.max(r[index]&255,rank);
 }
 public static double estimate(byte[] r) {
  double sum=0;int zero=0;for(byte b:r){sum+=Math.scalb(1.0,-(b&255));if(b==0)zero++;}
  double estimate=(0.7213/(1+1.079/4096))*4096*4096/sum;
  return estimate<=2.5*4096&&zero>0?4096*Math.log(4096.0/zero):estimate;
 }
}
