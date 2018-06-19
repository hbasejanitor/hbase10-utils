package org.hbasejanitor.flume.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;

public class DummyChannelProcessor extends ChannelProcessor {
  private List<Event> events = new ArrayList<>();
  
  public DummyChannelProcessor() {
    super(null);
  }
  
  @Override
  public void processEvent(Event event) {
    this.events.add(event);
  }
  
  @Override
  public void processEventBatch(List<Event> events) {
    this.events.addAll(events);
  }
  
  public List<Event> getEvents() {
    return events;
  }
}
