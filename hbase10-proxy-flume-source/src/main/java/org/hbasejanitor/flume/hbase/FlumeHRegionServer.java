package org.hbasejanitor.flume.hbase;


import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.*;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.regionserver.RpcSchedulerFactory;
import org.apache.hadoop.hbase.regionserver.SimpleRpcSchedulerFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.hbasejanitor.hbase.event.HBase10Event;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Stub region server that receives the replication events and if they pass the routing rules
 * forwards them to the configured kafka topic
 */
  public class FlumeHRegionServer
    implements AdminProtos.AdminService.BlockingInterface, Server, org.apache.hadoop.hbase.ipc.PriorityFunction {
  private static Log LOG = LogFactory.getLog(FlumeHRegionServer.class);
  private CuratorFramework zk;
  private Configuration hbaseConf;
  private RpcServer rpcServer;
  private String zkNodePath;
  private ServerName serverName;
  private boolean running = false;
  private ChannelProcessor channelProcessor;
  private DatumWriter<HBase10Event> avroWriter =
      new SpecificDatumWriter<HBase10Event>(HBase10Event.getClassSchema());

  private FlumeRoutingRules routingRules;
  
  private String baseZnode;
  
  private boolean addFlumeHeaders;
  
  public static final String REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS =
      "hbase.region.server.rpc.scheduler.factory.class";

  private Map<String, String> emptyMap = new HashMap<>();
  
  public FlumeHRegionServer() {

  }

  public FlumeHRegionServer(CuratorFramework zk, Configuration hbaseConf,
      ChannelProcessor channelProcessor, FlumeRoutingRules routeRules,String baseZnode,boolean addFlumeHeaders) {
    this.zk = zk;
    this.hbaseConf = hbaseConf;
    this.routingRules = routeRules;
    this.channelProcessor=channelProcessor;
    this.baseZnode=baseZnode;
    this.addFlumeHeaders=addFlumeHeaders;
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

      if (zk.checkExists().forPath(baseZnode+"/rs") == null) {
        zk.create().forPath(baseZnode+"/rs");
      }

      zkNodePath = baseZnode+"/rs/" + serverName.getServerName();
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
      zk.delete().deletingChildrenIfNeeded().forPath(zkNodePath);
      zk.close();
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

          if (routingRules.route(table, columnFamily, qualifier)) {
            byte[] key = CellUtil.cloneRow(cell);
            HBase10Event event = new HBase10Event();
            event.setKey(ByteBuffer.wrap(key));
            event.setDelete(CellUtil.isDelete(cell));
            event.setQualifier(ByteBuffer.wrap(qualifier));
            event.setFamily(ByteBuffer.wrap(columnFamily));
            event.setTable(ByteBuffer.wrap(entry.getKey().getTableName().toByteArray()));
            event.setValue(ByteBuffer.wrap(CellUtil.cloneValue(cell)));
            event.setTimestamp(cell.getTimestamp());
            
            Map<String,String> headers = emptyMap;
            
            if (addFlumeHeaders) {
              headers = new HashMap<>();
              headers.put("table",  table.toString());
              headers.put("columnfamily",  Bytes.toString(columnFamily));
              headers.put("qualifier",  Bytes.toString(qualifier));
            }
            pushFlumeMessage(event,headers);
          }

        }
        i++;
      }

      LOG.debug("pushed "+i+" messages to flume");
      
      
      return AdminProtos.ReplicateWALEntryResponse.newBuilder().build();
    } catch (Throwable ie) {
      LOG.error("Exception in service",ie);
      throw new ServiceException(ie);
    }
  }

    /**
     * Push the event to the flume channel
     * @param message
     * @param headers
     * @throws Exception
     */
  public void pushFlumeMessage(HBase10Event message,Map<String,String> headers)
      throws Exception {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bout, null);
    avroWriter.write(message, encoder);
    encoder.flush();
    
    byte[] value = bout.toByteArray();
    
    Event flumeEvent = EventBuilder.withBody(value);
    flumeEvent.setHeaders(headers);
    channelProcessor.processEvent(flumeEvent);
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
  public void setRoutingRules(FlumeRoutingRules routingRules) {
    this.routingRules = routingRules;
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

