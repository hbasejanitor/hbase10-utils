package org.hbasejanitor.hbase.kafka;

import org.hbasejanitor.hbase.event.HBase10Event;

public interface HbaseMutationSink {

  public void accept(byte key[], HBase10Event event);
  
  public void flush();
  
  public void close();
}
