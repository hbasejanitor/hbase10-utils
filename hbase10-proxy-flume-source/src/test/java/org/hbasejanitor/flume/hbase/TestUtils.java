package org.hbasejanitor.flume.hbase;

import java.nio.ByteBuffer;

import org.apache.commons.lang.SystemUtils;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;

public class TestUtils {
    private static HBaseTestingUtility utility = new HBaseTestingUtility();

    public static void shutdown() {
        try {
            utility.shutdownMiniCluster();
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static Connection setupAndGetConnection() {
        try {
            if (SystemUtils.IS_OS_WINDOWS) {
              System.setProperty("test.build.data.basedirectory","c:/temp");
            }
            
            utility.startMiniZKCluster(1,9011);
            utility.startMiniCluster();
            utility.createTable("testreplication1".getBytes(),new byte[][]{"cf1".getBytes(),"cf2".getBytes()});
            utility.createTable("testreplication2".getBytes(),new byte[][]{"cfa".getBytes(),"cfb".getBytes()});

            Connection hbaseCon=utility.getConnection();
            
            HTableDescriptor desc = hbaseCon.getAdmin().getTableDescriptor(TableName.valueOf("default:testreplication1"));

            HColumnDescriptor descc = desc.getFamily("cf1".getBytes())
                    .setScope(1);
            desc.modifyFamily(descc);
            
            HColumnDescriptor descc2 = desc.getFamily("cf2".getBytes())
                .setScope(1);
            desc.modifyFamily(descc2);
            
            hbaseCon.getAdmin().disableTable(TableName.valueOf("default:testreplication1"));
            hbaseCon.getAdmin().modifyTable(TableName.valueOf("default:testreplication1"),desc);
            hbaseCon.getAdmin().enableTable(TableName.valueOf("default:testreplication1"));
            
            NamespaceDescriptor ndesc = NamespaceDescriptor.create("testme").build(); 
            hbaseCon.getAdmin().createNamespace(ndesc);
            
            
            utility.createTable("testme:testnamespace1".getBytes(),new byte[][]{"cf1".getBytes(),"cf2".getBytes()});
            desc = hbaseCon.getAdmin().getTableDescriptor(TableName.valueOf("testme:testnamespace1"));

            descc = desc.getFamily("cf1".getBytes())
                    .setScope(1);
            desc.modifyFamily(descc);
            hbaseCon.getAdmin().disableTable(TableName.valueOf("testme:testnamespace1"));
            hbaseCon.getAdmin().modifyTable(TableName.valueOf("testme:testnamespace1"),desc);
            hbaseCon.getAdmin().enableTable(TableName.valueOf("testme:testnamespace1"));

            
            
            return hbaseCon;
        } catch ( Exception e){
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    
    public static String bytebufferToString(ByteBuffer buff) {
      byte strMe[]=new byte[buff.capacity()];
      buff.get(strMe);
      return Bytes.toString(strMe);
    }

}
