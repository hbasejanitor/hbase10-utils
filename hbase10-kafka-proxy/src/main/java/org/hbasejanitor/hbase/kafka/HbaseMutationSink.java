package org.hbasejanitor.hbase.kafka;

public interface HbaseMutationSink {

  public void accept(byte key[], HBaseKafkaEvent event);
  
  public void flush();
  
  public void close();
}
