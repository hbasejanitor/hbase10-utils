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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CloseRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CompactRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.FlushRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetOnlineRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetOnlineRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetServerInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetServerInfoResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetStoreFileRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetStoreFileResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.MergeRegionsRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.MergeRegionsResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.ReplicateWALEntryRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.ReplicateWALEntryResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.RollWALWriterRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.RollWALWriterResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.SplitRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.SplitRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.StopServerRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.StopServerResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.UpdateConfigurationRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.UpdateConfigurationResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.UpdateFavoredNodesRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.UpdateFavoredNodesResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WarmupRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WarmupRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.regionserver.RpcSchedulerFactory;
import org.apache.hadoop.hbase.regionserver.SimpleRpcSchedulerFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.zookeeper.CreateMode;
import org.hbasejanitor.hbase.event.HBase10Event;

import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Stub region server that receives the replication events and if they pass the routing rules
 * forwards them to the configured kafka topic
 */
public class ProxyHRegionServer
    implements AdminProtos.AdminService.BlockingInterface, Server, org.apache.hadoop.hbase.ipc.PriorityFunction {
  private static Log LOG = LogFactory.getLog(ProxyHRegionServer.class);
  private CuratorFramework zk;
  private Configuration hbaseConf;
  private RpcServer rpcServer;
  private String zkNodePath;
  private String peerName;
  private ServerName serverName;
  private boolean running = false;
  private Producer<byte[], byte[]> producer;
  private DatumWriter<HBase10Event> avroWriter =
      new SpecificDatumWriter<HBase10Event>(HBase10Event.getClassSchema());
  private TopicRoutingRules routingRules;
  
  public static final String REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS =
      "hbase.region.server.rpc.scheduler.factory.class";

  public ProxyHRegionServer() {

  }

  public ProxyHRegionServer(CuratorFramework zk, Configuration hbaseConf, String peerName,
      Producer<byte[], byte[]> producer, TopicRoutingRules routeRules) {
    this.zk = zk;
    this.hbaseConf = hbaseConf;
    this.peerName = peerName;
    this.producer = producer;
    this.routingRules = routeRules;
  }
  
  /**
   * start the proxy region server
   * @throws Exception the proxy could not start
   */
  public void start() throws Exception {
    try {
      RpcSchedulerFactory rpcSchedulerFactory;      
      Class<?> rpcSchedulerFactoryClass = hbaseConf.getClass(
          REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS,
          SimpleRpcSchedulerFactory.class);
      rpcSchedulerFactory = ((RpcSchedulerFactory) rpcSchedulerFactoryClass.newInstance());

      String hostName = Strings.domainNamePointerToHostName(
        DNS.getDefaultHost(hbaseConf.get("hbase.regionserver.dns.interface", "default"),
          hbaseConf.get("hbase.regionserver.dns.nameserver", "default")));
      
      LOG.info("listening on host is " + hostName);

      InetSocketAddress initialIsa = new InetSocketAddress(hostName, 0);
      if (initialIsa.getAddress() == null) {
        throw new IllegalArgumentException("Failed resolve of " + initialIsa);
      }
      String name = "regionserver/" + initialIsa.toString();

    this.rpcServer = new RpcServer(
      this, 
      name, 
      getServices(), 
      initialIsa, 
      hbaseConf, 
      rpcSchedulerFactory.create(hbaseConf, this, this));

      
      this.serverName = ServerName.valueOf(hostName, rpcServer.getListenerAddress().getPort(),
        System.currentTimeMillis());

      rpcServer.start();
      // Publish our existence in ZooKeeper

      if (zk.checkExists().forPath("/hbasekafkaproxy/rs") == null) {
        zk.create().forPath("/hbasekafkaproxy/rs");
      }

      zkNodePath = "/hbasekafkaproxy/rs/" + serverName.getServerName();
      zk.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(zkNodePath);
    } catch (Exception e) {
      LOG.error("Exception when attempting to start proxy ",e);
      throw new IOException(e);
    }
    this.running = true;
  }

  /**
   * stop the proxy server
   */
  public void stop() {
    try {
      rpcServer.stop();
      producer.flush();
      producer.close();
      zk.delete().deletingChildrenIfNeeded().forPath(zkNodePath);
    } catch (Exception e) {
      LOG.error("Exception occured when shutting down proxy", e);
    }

  }

  /**
   * Handle the replication events that are coming in.
   * @param controller contains the cells for the mutations
   * @param request contains the WALLEntry objects
   * @return ReplicateWALEntryResponse replicateWALEntry on success
   * @throws ServiceException if entries could not be processed
   */
  public ReplicateWALEntryResponse replicateWALEntry(
      RpcController controller,
      ReplicateWALEntryRequest request)
      throws com.google.protobuf.ServiceException {
    try {
      List<AdminProtos.WALEntry> entries = request.getEntryList();
      CellScanner cells = ((PayloadCarryingRpcController) controller).cellScanner();

      int i = 0;

      for (final AdminProtos.WALEntry entry : entries) {
        int count = entry.getAssociatedCellCount();

        for (int y = 0; y < count; y++) {
          if (!cells.advance()) {
            throw new ArrayIndexOutOfBoundsException("Expected=" + count + ", index=" + i);
          }

          TableName table = TableName.valueOf(entry.getKey().getTableName().toByteArray());
          Cell cell = cells.current();
          byte[] columnFamily = CellUtil.cloneFamily(cell);
          byte[] qualifier = CellUtil.cloneQualifier(cell);

          List<String> topics = null;

          if (!routingRules.isExclude(table, columnFamily, qualifier)) {
            topics = routingRules.getTopics(table, columnFamily, qualifier);
          }

          if (!CollectionUtils.isEmpty(topics)) {
            byte[] key = CellUtil.cloneRow(cell);
            HBase10Event event = new HBase10Event();
            event.setKey(ByteBuffer.wrap(key));
            event.setDelete(CellUtil.isDelete(cell));
            event.setQualifier(ByteBuffer.wrap(qualifier));
            event.setFamily(ByteBuffer.wrap(columnFamily));
            event.setTable(ByteBuffer.wrap(entry.getKey().getTableName().toByteArray()));
            event.setValue(ByteBuffer.wrap(CellUtil.cloneValue(cell)));
            event.setTimestamp(cell.getTimestamp());
            pushKafkaMessage(event, key, topics);
          }
        }
        i++;
      }

      producer.flush();
      
      LOG.debug("pushed "+i+" messages to kafka");
      
      
      return AdminProtos.ReplicateWALEntryResponse.newBuilder().build();
    } catch (Throwable ie) {
      LOG.error("Exception in service",ie);
      throw new ServiceException(ie);
    }
  }

  /**
   * push the message to the topic(s)
   * @param message message to publish
   * @param key key for kafka topic
   * @param topics topics to publish to
   * @throws Exception if message oculd not be sent
   */
  public void pushKafkaMessage(HBase10Event message, byte[] key, List<String> topics)
      throws Exception {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bout, null);
    avroWriter.write(message, encoder);
    encoder.flush();

    byte[] value = bout.toByteArray();

    for (String topic : topics) {
      ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(topic, key, value);
      producer.send(record);
    }
  }

  /**
   * @return list of blocking services and their security info classes that this server supports
   */
  protected List<BlockingServiceAndInterface> getServices() {
    List<RpcServer.BlockingServiceAndInterface> bssi = new ArrayList<RpcServer.BlockingServiceAndInterface>(1);
    bssi.add(new RpcServer.BlockingServiceAndInterface(
            AdminProtos.AdminService.newReflectiveBlockingService(this),
            AdminProtos.AdminService.BlockingInterface.class));
    return bssi;
  }
  
  /**
   * set the routing rules (used for unit tests)
   * @param routingRules routing tules to use
   */
  public void setRoutingRules(TopicRoutingRules routingRules) {
    this.routingRules = routingRules;
  }

  /**
   * set the kafka producer (used for unit tests)
   * @param producer producer to use
   */
  public void setProducer(Producer<byte[], byte[]> producer) {
    this.producer = producer;
  }

  public void abort(String why, Throwable e) {
    LOG.error("STOP! ",e);
    this.stop();
  }

  public boolean isAborted() {
    return !this.running;
  }

  public void stop(String why) {
    LOG.info("STOP called "+why);
    this.stop();
  }

  public boolean isStopped() {
    return !this.running;
  }

  public Configuration getConfiguration() {
    return hbaseConf;
  }

  @Override
  public int getPriority(RequestHeader header, Message param, User user) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getDeadline(RequestHeader header, Message param) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public ZooKeeperWatcher getZooKeeper() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ClusterConnection getConnection() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public MetaTableLocator getMetaTableLocator() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ServerName getServerName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CoordinatedStateManager getCoordinatedStateManager() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChoreService getChoreService() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GetRegionInfoResponse getRegionInfo(RpcController controller, GetRegionInfoRequest request)
      throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GetStoreFileResponse getStoreFile(RpcController controller, GetStoreFileRequest request)
      throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GetOnlineRegionResponse getOnlineRegion(RpcController controller,
      GetOnlineRegionRequest request) throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public OpenRegionResponse openRegion(RpcController controller, OpenRegionRequest request)
      throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public WarmupRegionResponse warmupRegion(RpcController controller, WarmupRegionRequest request)
      throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CloseRegionResponse closeRegion(RpcController controller, CloseRegionRequest request)
      throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public FlushRegionResponse flushRegion(RpcController controller, FlushRegionRequest request)
      throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SplitRegionResponse splitRegion(RpcController controller, SplitRegionRequest request)
      throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CompactRegionResponse compactRegion(RpcController controller, CompactRegionRequest request)
      throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public MergeRegionsResponse mergeRegions(RpcController controller, MergeRegionsRequest request)
      throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ReplicateWALEntryResponse replay(RpcController controller,
      ReplicateWALEntryRequest request) throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RollWALWriterResponse rollWALWriter(RpcController controller, RollWALWriterRequest request)
      throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GetServerInfoResponse getServerInfo(RpcController controller, GetServerInfoRequest request)
      throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public StopServerResponse stopServer(RpcController controller, StopServerRequest request)
      throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public UpdateFavoredNodesResponse updateFavoredNodes(RpcController controller,
      UpdateFavoredNodesRequest request) throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public UpdateConfigurationResponse updateConfiguration(RpcController controller,
      UpdateConfigurationRequest request) throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }
  
}


