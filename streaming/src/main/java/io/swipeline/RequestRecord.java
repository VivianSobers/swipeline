package io.swipeline;

import java.nio.charset.StandardCharsets;

/** Request bytes and source coordinates retained across feature enrichment. */
public class RequestRecord {
 public String value;
 public byte[] raw;
 public int partition;
 public long offset;
 public RequestRecord() {}
 public RequestRecord(byte[] raw,int partition,long offset) {
  this.raw=raw;this.partition=partition;this.offset=offset;
  this.value=raw==null?"":new String(raw,StandardCharsets.UTF_8);
 }
 public RequestRecord enriched(String value) {
  RequestRecord next=new RequestRecord(raw,partition,offset);next.value=value;return next;
 }
 public String quarantine(String error,long now) throws Exception {
  return Wire.quarantine(raw==null?new byte[0]:raw,partition,offset,error,now);
 }
}
