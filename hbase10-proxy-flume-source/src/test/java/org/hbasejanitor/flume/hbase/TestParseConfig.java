package org.hbasejanitor.flume.hbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import junit.framework.TestCase;

public class TestParseConfig extends TestCase {
  // rules come in this form:
  // <agent id>.rule.<rule id>.keep=true/false (defaults to true)
  // <agent id>.rule.<rule id>.rules.columnfamily=<column family>
  // <agent id>.rule.<rule id>.rules.qualifier=<qualifier>
  // <agent id>.rule.<rule id>.rules.table=<table>

  public void testSimpleConf() {
    
    Map<String,String> values = new HashMap<>();
    values.put("rule.1.keep", "true");
    values.put("rule.1.rules.table", "default:foo");
    values.put("rule.1.rules.qualifier", "turrible");
    
    
    values.put("rule.2.keep", "false");
    values.put("rule.2.rules.table", "default:foo1");
    values.put("rule.2.rules.qualifier", "notturrible");
    
    values.put("rule.3.rules.table", "default:foo1");
    values.put("rule.3.keep", "true");
    
    values.put("rule.4.rules.table", "default:foo1");
    values.put("rule.4.keep", "false");
    values.put("rule.4.rules.qualifier", "bo*");
    
    values.put("rule.5.rules.table", "default:foo1");
    values.put("rule.5.keep", "false");
    values.put("rule.5.rules.qualifier", "*bo");

    
    Context context = new Context(values);
    
    FlumeRoutingRules rules = Hbase10PeerFlumeSource.parseRoutingRules(context);
    
    TableName table = TableName.valueOf("default", "foo");
    byte qualifier[] = Bytes.toBytes("turrible");
    byte columnFamily[]=Bytes.toBytes("blahblahblah");
    
    assertTrue(rules.route(table, columnFamily, qualifier));
    
    context = new Context(values);
    
    table = TableName.valueOf("default", "foo1");
    qualifier = Bytes.toBytes("turrible");
    columnFamily=Bytes.toBytes("blahblahblah");
    
    assertTrue(rules.route(table, columnFamily, qualifier));
    
    table = TableName.valueOf("default", "foo1");
    qualifier = Bytes.toBytes("notturrible");
    columnFamily=Bytes.toBytes("blahblahblah");
    
    assertFalse(rules.route(table, columnFamily, qualifier));
    
    table = TableName.valueOf("default", "foo1");
    qualifier = Bytes.toBytes("bobob");
    columnFamily=Bytes.toBytes("yikes");
    
    assertFalse(rules.route(table, columnFamily, qualifier));

    table = TableName.valueOf("default", "foo1");
    qualifier = Bytes.toBytes("yobo");
    columnFamily=Bytes.toBytes("yikes");
    
    assertFalse(rules.route(table, columnFamily, qualifier));

//    Put p = new Put("".getBytes());
//    List<Tag> tagz = new ArrayList<>();
//    Tag taggerd = new Tag(tagType, tag)
//    KeyValue c = new KeyValue("row".getBytes(),"family".getBytes(),"qualifier".getBytes(),System.currentTimeMillis(),"value".getBytes(),tagz);
   
    
  }
  

  
}


 