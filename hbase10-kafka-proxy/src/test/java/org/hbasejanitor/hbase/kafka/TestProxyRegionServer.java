/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.hbasejanitor.hbase.kafka;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.junit.Assert;
import org.junit.Test;

import com.google.protobuf.ByteString;


/**
 * Test ProxyHRegionServer
 */
public class TestProxyRegionServer {

  public AdminProtos.ReplicateWALEntryRequest makeReplicateWALEntryRequest(String table,
      int cellcount) {
    WALProtos.WALKey key1 = WALProtos.WALKey.newBuilder()
        .setTableName(ByteString.copyFrom(table.getBytes())).buildPartial();

    AdminProtos.WALEntry entry = AdminProtos.WALEntry.newBuilder().setAssociatedCellCount(cellcount)
        .setKey(key1).buildPartial();

    AdminProtos.ReplicateWALEntryRequest req =
        AdminProtos.ReplicateWALEntryRequest.newBuilder().addEntry(entry).buildPartial();
    return req;
  }

  @Test
  public void testReplicationEventNoRules() {
    ProxyHRegionServer myReplicationProxy = new ProxyHRegionServer();
    List<Cell> cells = new ArrayList<>();
    KeyValue kv = new KeyValue("row".getBytes(), "family".getBytes(), "qualifier".getBytes(), 1,
        "value".getBytes());
    cells.add(kv);

    AdminProtos.ReplicateWALEntryRequest req =
        makeReplicateWALEntryRequest("default:Mikey", cells.size());

    TopicRoutingRules rules = new TopicRoutingRules();
    rules.parseRules(new ByteArrayInputStream("<rules></rules>".getBytes()));

    ProducerForTesting mockProducer = new ProducerForTesting();
    myReplicationProxy.setProducer(mockProducer);
    myReplicationProxy.setRoutingRules(rules);

    PayloadCarryingRpcController rpcMock = new PayloadCarryingRpcControllerMock(cells);

    try {
      myReplicationProxy.replicateWALEntry(rpcMock, req);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    // no rules, so this should be empty.
    Assert.assertEquals(0, mockProducer.getMessages().size());
  }

  @Test
  public void testDropEvent() {
    ProxyHRegionServer myReplicationProxy = new ProxyHRegionServer();
    List<Cell> cells = new ArrayList<>();
    KeyValue kv = new KeyValue("row1".getBytes(), "family".getBytes(), "qualifier".getBytes(), 1,
        "value".getBytes());
    cells.add(kv);

    kv = new KeyValue("row2".getBytes(), "family".getBytes(), "qualifier".getBytes(), 1,
        "value".getBytes());

    AdminProtos.ReplicateWALEntryRequest req =
        makeReplicateWALEntryRequest("default:Mikey", cells.size());

    TopicRoutingRules rules = new TopicRoutingRules();
    rules.parseRules(new ByteArrayInputStream(
        "<rules><rule action=\"drop\" table=\"default:Mikey\" /></rules>".getBytes()));

    ProducerForTesting mockProducer = new ProducerForTesting();
    myReplicationProxy.setProducer(mockProducer);
    myReplicationProxy.setRoutingRules(rules);

    PayloadCarryingRpcController rpcMock = new PayloadCarryingRpcControllerMock(cells);

    try {
      myReplicationProxy.replicateWALEntry(rpcMock, req);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    // no rules, so this should be empty.
    Assert.assertEquals(0, mockProducer.getMessages().size());
  }

  @Test
  public void testRouteBothEvents() {
    ProxyHRegionServer myReplicationProxy = new ProxyHRegionServer();
    List<Cell> cells = new ArrayList<>();
    KeyValue kv = new KeyValue("row1".getBytes(), "family1".getBytes(), "qualifier1".getBytes(), 1,
        "value1".getBytes());
    cells.add(kv);

    kv = new KeyValue("row2".getBytes(), "family2".getBytes(), "qualifier2".getBytes(), 2,
        "value2".getBytes());
    cells.add(kv);

    AdminProtos.ReplicateWALEntryRequest req =
        makeReplicateWALEntryRequest("default:Mikey", cells.size());

    TopicRoutingRules rules = new TopicRoutingRules();
    rules.parseRules(new ByteArrayInputStream(
        "<rules><rule action=\"route\" table=\"default:Mikey\" topic=\"foobar\"/></rules>"
            .getBytes()));

    ProducerForTesting mockProducer = new ProducerForTesting();
    myReplicationProxy.setProducer(mockProducer);
    myReplicationProxy.setRoutingRules(rules);

    PayloadCarryingRpcController rpcMock = new PayloadCarryingRpcControllerMock(cells);

    try {
      myReplicationProxy.replicateWALEntry(rpcMock, req);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    // we routed both to topic foobar
    Assert.assertEquals(1, mockProducer.getMessages().size());
    Assert.assertEquals(2, mockProducer.getMessages().get("foobar").size());

    String valueCheck =
        new String(mockProducer.getMessages().get("foobar").get(0).getKey().array());
    Assert.assertEquals("row1", valueCheck);
    valueCheck = new String(mockProducer.getMessages().get("foobar").get(0).getFamily().array());
    Assert.assertEquals("family1", valueCheck);
    valueCheck = new String(mockProducer.getMessages().get("foobar").get(0).getQualifier().array());
    Assert.assertEquals("qualifier1", valueCheck);
    valueCheck = new String(mockProducer.getMessages().get("foobar").get(0).getValue().array());
    Assert.assertEquals("value1", valueCheck);

    valueCheck = new String(mockProducer.getMessages().get("foobar").get(1).getKey().array());
    Assert.assertEquals("row2", valueCheck);
    valueCheck = new String(mockProducer.getMessages().get("foobar").get(1).getFamily().array());
    Assert.assertEquals("family2", valueCheck);
    valueCheck = new String(mockProducer.getMessages().get("foobar").get(1).getQualifier().array());
    Assert.assertEquals("qualifier2", valueCheck);
    valueCheck = new String(mockProducer.getMessages().get("foobar").get(1).getValue().array());
    Assert.assertEquals("value2", valueCheck);
  }

  @Test
  public void testRoute1Event() {
    ProxyHRegionServer myReplicationProxy = new ProxyHRegionServer();
    List<Cell> cells = new ArrayList<>();
    KeyValue kv = new KeyValue("row1".getBytes(), "family1".getBytes(), "qualifier1".getBytes(), 1,
        "value1".getBytes());
    cells.add(kv);

    /*
     * public KeyValue(final byte[] row, final byte[] family, final byte[] qualifier, final long
     * timestamp, final byte[] value) { this(row, family, qualifier, timestamp, Type.Put, value); }
     */

    kv = new KeyValue("row2".getBytes(), "family2".getBytes(), "qualifier2".getBytes(),

        2, "value2".getBytes());
    cells.add(kv);

    AdminProtos.ReplicateWALEntryRequest req =
        makeReplicateWALEntryRequest("default:Mikey", cells.size());

    TopicRoutingRules rules = new TopicRoutingRules();
    rules.parseRules(new ByteArrayInputStream(
        ("<rules><rule action=\"route\" table=\"default:Mikey\" "
            + "topic=\"foobar\" qualifier=\"qualifier2\" /></rules>")
            .getBytes()));

    ProducerForTesting mockProducer = new ProducerForTesting();
    myReplicationProxy.setProducer(mockProducer);
    myReplicationProxy.setRoutingRules(rules);

    PayloadCarryingRpcController rpcMock = new PayloadCarryingRpcControllerMock(cells);

    try {
      myReplicationProxy.replicateWALEntry(rpcMock, req);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    // we routed both to topic foobar
    Assert.assertEquals(1, mockProducer.getMessages().size());
    Assert.assertEquals(1, mockProducer.getMessages().get("foobar").size());

    String valueCheck =
        new String(mockProducer.getMessages().get("foobar").get(0).getKey().array());
    Assert.assertEquals("row2", valueCheck);
    valueCheck = new String(mockProducer.getMessages().get("foobar").get(0).getFamily().array());
    Assert.assertEquals("family2", valueCheck);
    valueCheck = new String(mockProducer.getMessages().get("foobar").get(0).getQualifier().array());
    Assert.assertEquals("qualifier2", valueCheck);
    valueCheck = new String(mockProducer.getMessages().get("foobar").get(0).getValue().array());
    Assert.assertEquals("value2", valueCheck);
  }

  @Test
  public void testRouteBothEvents2() {
    ProxyHRegionServer myReplicationProxy = new ProxyHRegionServer();
    List<Cell> cells = new ArrayList<>();
    KeyValue kv = new KeyValue("row1".getBytes(), "family1".getBytes(), "qualifier1".getBytes(), 1,
        "value1".getBytes());
    cells.add(kv);

    kv = new KeyValue("row2".getBytes(), "family2".getBytes(), "qualifier2".getBytes(), 2,
        "value2".getBytes());
    cells.add(kv);

    AdminProtos.ReplicateWALEntryRequest req =
        makeReplicateWALEntryRequest("default:Mikey", cells.size());

    TopicRoutingRules rules = new TopicRoutingRules();
    rules.parseRules(new ByteArrayInputStream(
        ("<rules><rule action=\"route\" table=\"default:Mikey\" "
            + "qualifier=\"qualifier*\" topic=\"foobar\"/></rules>")
            .getBytes()));

    ProducerForTesting mockProducer = new ProducerForTesting();
    myReplicationProxy.setProducer(mockProducer);
    myReplicationProxy.setRoutingRules(rules);

    PayloadCarryingRpcController rpcMock = new PayloadCarryingRpcControllerMock(cells);

    try {
      myReplicationProxy.replicateWALEntry(rpcMock, req);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    // we routed both to topic foobar (using a wildcarded qualifier)
    Assert.assertEquals(1, mockProducer.getMessages().size());
    Assert.assertEquals(2, mockProducer.getMessages().get("foobar").size());

    String valueCheck =
        new String(mockProducer.getMessages().get("foobar").get(0).getKey().array());
    Assert.assertEquals("row1", valueCheck);
    valueCheck = new String(mockProducer.getMessages().get("foobar").get(0).getFamily().array());
    Assert.assertEquals("family1", valueCheck);
    valueCheck = new String(mockProducer.getMessages().get("foobar").get(0).getQualifier().array());
    Assert.assertEquals("qualifier1", valueCheck);
    valueCheck = new String(mockProducer.getMessages().get("foobar").get(0).getValue().array());
    Assert.assertEquals("value1", valueCheck);

    valueCheck = new String(mockProducer.getMessages().get("foobar").get(1).getKey().array());
    Assert.assertEquals("row2", valueCheck);
    valueCheck = new String(mockProducer.getMessages().get("foobar").get(1).getFamily().array());
    Assert.assertEquals("family2", valueCheck);
    valueCheck = new String(mockProducer.getMessages().get("foobar").get(1).getQualifier().array());
    Assert.assertEquals("qualifier2", valueCheck);
    valueCheck = new String(mockProducer.getMessages().get("foobar").get(1).getValue().array());
    Assert.assertEquals("value2", valueCheck);
  }

}