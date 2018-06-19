package org.hbasejanitor.flume.hbase;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.Validate;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Hbase10PeerFlumeSource implements EventDrivenSource, Configurable {

  
  private static final Logger LOG = LoggerFactory.getLogger(Hbase10PeerFlumeSource.class);
  private ChannelProcessor channelProcessor;
  private FlumeHRegionServer peer;
  private String peerName1;
  private FlumeHRegionServer flumeHRegionServer;
  private String baseZnode="/hbaseflumepeer";
  private CuratorFramework zk;
  private Configuration hconf;
  private FlumeRoutingRules flumeRoutingRules;
  private ReplicationAdmin replicationAdmin;
  
  private boolean autoEnable=false;
  private String name;
  private boolean addHeaders; 
  
  
  private void setUpZnodes() throws Exception {
    this.hconf = HBaseConfiguration.create();
    String zookeeperQ = this.hconf.get("hbase.zookeeper.quorum") + ":" +
        this.hconf.get("hbase.zookeeper.property.clientPort");
    ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(20000, 20);
    zk = CuratorFrameworkFactory.newClient(zookeeperQ, retryPolicy);
    zk.start();
    // always gives the same uuid for the same name
    UUID uuid = UUID.nameUUIDFromBytes(Bytes.toBytes(peerName1));
    byte []uuidBytes = Bytes.toBytes(uuid.toString());
    String idPath=baseZnode+"/hbaseid";
    if (zk.checkExists().forPath(idPath) == null) {
      zk.create().creatingParentsIfNeeded().forPath(baseZnode +
          "/hbaseid",uuidBytes);
    } else {
      // If the znode is there already make sure it has the
      // expected value for the peer name.
      if (!Bytes.equals(zk.getData().forPath(idPath).clone(),uuidBytes)){
        LOG.warn("znode "+idPath+" has unexpected value "
            + " (did the peer name for the proxy change?) "
            + "Updating value");
        zk.setData().forPath(idPath, uuidBytes);
      }
    }

    if (zk.checkExists().forPath(baseZnode + "/rs") == null) {
      zk.create().forPath(baseZnode+ "/rs");
    }
  }
  
  public void createRegionServer() throws Exception {
    this.flumeHRegionServer = new FlumeHRegionServer(zk,
      this.hconf,
      this.channelProcessor,
      flumeRoutingRules,
      this.baseZnode,
      this.addHeaders);
    this.flumeHRegionServer.start();
  }
  
  public static FlumeRoutingRules parseRoutingRules(Context context) {
    // rules come in this form:
    // <agent id>.rule.<rule id>.keep=true/false
    // <agent id>.rule.<rule id>.rules.columnfamily=<column family>
    // <agent id>.rule.<rule id>.rules.qualifier=<qualifier>
    // <agent id>.rule.<rule id>.rules.table=<table>
    FlumeRoutingRules parsedRules = new FlumeRoutingRules();
        
    
    List<String> ruleNumbers =context.getSubProperties("rule.").keySet()
      .stream()
      .map((line) -> {Validate.isTrue(Character.isDigit(line.charAt(0))); return line;})
      .map((line) -> line.substring(0, line.indexOf('.')))
      .sorted()
      .distinct().collect(Collectors.toList());
    
    
    LOG.info("rule numbers "+ruleNumbers);
    LOG.info("context "+context.toString());
    
    ruleNumbers.stream()
      .map((ruleNumber)->new Pair<>(ruleNumber,context.getSubProperties("rule."+ruleNumber+".rules.")))
      .forEach((mapOfProps)->{
        Rule r= new Rule();
        if (mapOfProps.getSecond().containsKey("table")) {
          r.tableName=TableName.valueOf(mapOfProps.getSecond().get("table"));
        }
        if (mapOfProps.getSecond().containsKey("columnfamily")) {
          r.setColumnFamily(mapOfProps.getSecond().get("columnfamily"));
        }
        if (mapOfProps.getSecond().containsKey("qualifier")) {
          r.setQualifier(mapOfProps.getSecond().get("qualifier"));
        }
        if (context.getBoolean("rule."+mapOfProps.getFirst()+".keep", false)) {
          parsedRules.addRouteRule(r);
        } else {
          parsedRules.addDropRule(r);
        }
     });

    LOG.info("MY rules after parsing "+parsedRules);
    
    return parsedRules;
  }
  
  @Override
  public void configure(Context context) {
    this.flumeRoutingRules=parseRoutingRules(context);
    this.peerName1=context.getString("hbase.peer");
    this.addHeaders=context.getBoolean("flume.addheaders",false);
    this.autoEnable=context.getBoolean("hbase.enablepeer",false);
    Validate.isTrue(this.peerName1 != null,"peer name can not be null!");
  }
  
  @Override
  public void start() {
    try {
      setUpZnodes();
      createRegionServer();
      createEnablePeer(HBaseConfiguration.create(),this.baseZnode,this.autoEnable);
      
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * create peer if not there (
   *  (controlled by createIfMissing)
   * @param hbaseConf the hbase configuratoin
   * @param zk CuratorFramework object
   * @param basePath base znode.
   * @param internalName id if the peer to check for/create.
   * @param createIfMissing if the peer doesn't exist, create it and peer to it.
   */
  public void createEnablePeer(Configuration hbaseConf,
                              String basePath,boolean enablePeer) {

    
    ReplicationPeerConfig peerConfig = null;
    try  {
      this.replicationAdmin = new ReplicationAdmin(hbaseConf);
      this.replicationAdmin.getPeerConfig(peerName1);
    } catch (Exception e) {
    }
    
    try {
      if (peerConfig ==null) {
        LOG.info("need to create peer "+this.peerName1);
        peerConfig = new ReplicationPeerConfig();
        // get the current cluster's ZK config
        String zookeeperQ = hbaseConf.get("hbase.zookeeper.quorum") +
                    ":" + hbaseConf.get("hbase.zookeeper.property.clientPort");
        String znodePath = zookeeperQ + ":"+basePath;
        peerConfig.setClusterKey(znodePath);
        this.replicationAdmin.addPeer(peerName1, peerConfig, null);
        LOG.info("Created peer "+this.peerName1);
      }
      if ((enablePeer)&&(!this.replicationAdmin.getPeerState(peerName1))) {
        LOG.info("Enable peer "+peerName1);
        this.replicationAdmin.enablePeer(peerName1);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }  
  
  
  
  /**
   * create peer if not there (
   *  (controlled by createIfMissing)
   * @param hbaseConf the hbase configuratoin
   * @param zk CuratorFramework object
   * @param basePath base znode.
   * @param internalName id if the peer to check for/create.
   * @param createIfMissing if the peer doesn't exist, create it and peer to it.
   */
  public void disablePeer(Configuration hbaseConf,String peerName) {
    try  {
          LOG.info("disable peer "+peerName);
          this.replicationAdmin.disablePeer(peerName);
      } catch (Exception e) {
          LOG.error("Exception disabling peer",e);
      }
  }  
  
  @Override
  public LifecycleState getLifecycleState() {
    if  (flumeHRegionServer==null)
      return LifecycleState.STOP;
    if (flumeHRegionServer.isStopped()) {
       return LifecycleState.STOP;
    }
    return LifecycleState.START;
  }

  
  @Override
  public void stop() {
    if (flumeHRegionServer !=null) {
      this.flumeHRegionServer.stop("FLUME said so");
    }
    // check for the peer, if it is active, pause it.
    this.disablePeer(HBaseConfiguration.create(), this.peerName1);
    IOUtils.closeQuietly(replicationAdmin);
  }

  public void setPeer(FlumeHRegionServer peer) {
    this.peer = peer;
  }
  
  @Override
  public void setChannelProcessor(ChannelProcessor channelProcessor) {
    this.channelProcessor=channelProcessor;
  }

  @Override
  public ChannelProcessor getChannelProcessor() {
    return this.channelProcessor;
  }

  
  @Override
  public void setName(String name) {
    this.name=name;
  }

  @Override
  public String getName() {
    return name;
  }


}
