package  org.hbasejanitor.flume.hbase;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import junit.framework.TestCase;
import org.hbasejanitor.hbase.event.HBase10Event;

public class TestGetReplication extends TestCase {
  
  Connection connection;
  
  @Override
  protected void setUp() throws Exception {
    connection = TestUtils.setupAndGetConnection();
  }

  @Override
  protected void tearDown() throws Exception {
    connection.close();
    TestUtils.shutdown();
  }
  
  public void testReplication1() {
    
    
    Hbase10PeerFlumeSource flumeSource = new Hbase10PeerFlumeSource();
    flumeSource.setName("testyflume");
    Map<String,String> values = new HashMap<>();
    values.put("hbase.peer", "testpeer");
    values.put("hbase.enablepeer", "true");
    values.put("flume.addheaders", "true");
    values.put("rule.1.keep", "true");
    values.put("rule.1.rules.table", "default:testreplication1");
    values.put("rule.1.rules.qualifier", "good");
    Context context = new Context(values);
    DummyChannelProcessor dummy = new DummyChannelProcessor();
    flumeSource.setChannelProcessor(dummy);  
    flumeSource.configure(context);
    
    try (ReplicationAdmin repAdmin = new ReplicationAdmin(connection.getConfiguration())){
      flumeSource.start();

      
      assertEquals(true,repAdmin.getPeerState("testpeer"));
      
      try (Table t = connection.getTable(TableName.valueOf("default:testreplication1"))){
        Put p = new Put("put1".getBytes());
        p.addColumn("cf1".getBytes(), "good".getBytes(), "good value!".getBytes());
        t.put(p);
        
        Thread.sleep(10000);
        
        Event flumeEvent = dummy.getEvents().remove(0);
        assertEquals(flumeEvent.getHeaders().get("table"),"testreplication1");
        assertEquals(flumeEvent.getHeaders().get("columnfamily"),"cf1");
        assertEquals(flumeEvent.getHeaders().get("qualifier"),"good");
        
        DatumReader<HBase10Event> avroReader =
            new SpecificDatumReader<HBase10Event>(HBase10Event.getClassSchema());
        
        byte body[]=flumeEvent.getBody();
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(body, null);
        HBase10Event ev = avroReader.read(null,decoder);
        
        assertEquals("testreplication1", TestUtils.bytebufferToString(ev.getTable()));
        assertEquals("cf1", TestUtils.bytebufferToString(ev.getFamily()));
        assertEquals("good", TestUtils.bytebufferToString(ev.getQualifier()));
        assertEquals("good value!", TestUtils.bytebufferToString(ev.getValue()));
        
        assertTrue(ev.getTimestamp()>0);

        
        p = new Put("put1".getBytes());
        p.addColumn("cf1".getBytes(), "bad".getBytes(), "not so much.".getBytes());
        t.put(p);

        Thread.sleep(10000);
        assertTrue(dummy.getEvents().isEmpty());

        Get g = new Get("put1".getBytes());
        Result r = t.get(g);
        assertEquals("good value!",Bytes.toString(CellUtil.cloneValue(r.getColumnLatestCell("cf1".getBytes(), "good".getBytes()))));
        assertEquals("not so much.",Bytes.toString(CellUtil.cloneValue(r.getColumnLatestCell("cf1".getBytes(), "bad".getBytes()))));
        
        
        
        //HBase10Event ev 
      } catch (Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      flumeSource.stop();
    }
    
    
    
  }
}
