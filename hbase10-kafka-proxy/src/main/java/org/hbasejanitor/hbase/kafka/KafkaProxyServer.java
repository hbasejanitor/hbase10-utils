/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hbasejanitor.hbase.kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * hbase to kafka bridge.
 *
 * Starts up a region server and receives replication events, just like a peer
 * cluster member.  It takes the events and cell by cell determines how to
 * route them (see kafka-route-rules.xml)
 */
public class KafkaProxyServer extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(KafkaProxyServer.class);

  public static void main(String[] args) {
    try {
      ToolRunner.run(new KafkaProxyServer(), args);
    } catch (Exception e) {
      LOG.error("Exception running proxy ",e);
    }
  }

  private static void printUsageAndExit(Options options, int exitCode) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("hbase org.hbasejanitor.hbase.kafka.KafkaProxyServer", "", options,"", true);
    System.exit(exitCode);
  }

  @Override
  public int run(String[] args) {
    LOG.info("***** STARTING service '" + KafkaProxyServer.class.getSimpleName() + "' *****");
    VersionInfo.logVersion();

    Options options = new Options();

    options.addOption("b", "kafkabrokers", true,
            "Kafka Brokers (comma delimited)");
    options.addOption("p", "peername", true,
            "Name of hbase peer");
    options.addOption("a", "autopeer", false,
            "Create a peer auotmatically to the hbase cluster");
    options.addOption("f", "kafkaproperties", false,
            "Path to properties file that has the kafka connection properties");

    Option opt = new Option("r", "routerulesfile", true, "file that has routing rules");
    opt.setRequired(true);
    options.addOption(opt);
    
    CommandLine commandLine = null;
    try {
      commandLine = new PosixParser().parse(options, args);
    } catch (ParseException e) {
      LOG.error("Could not parse: ", e);
      printUsageAndExit(options, -1);
      System.exit(-1);
    }

    boolean autoPeer = (commandLine.getOptionValue('p') ==null);

    final Configuration conf = HBaseConfiguration.create(getConf());
    String zookeeperQ = conf.get("hbase.zookeeper.quorum") + ":" +
            conf.get("hbase.zookeeper.property.clientPort");
    String kafkaServers = commandLine.getOptionValue('b');

    String routeRulesFile = commandLine.getOptionValue('r');

    TopicRoutingRules routingRules = new TopicRoutingRules();
    try (FileInputStream fin = new FileInputStream(routeRulesFile);){
      routingRules.parseRules(fin);
    } catch (IOException e) {
      LOG.error("Rule file " + routeRulesFile + " not found or invalid");
      System.exit(-1);
    }

    if ((commandLine.getOptionValue('f')==null)&&(commandLine.getOptionValue('b')==null)) {
      System.err.println("You must provide a list of kafka brokers or a properties " +
              "file with the connection properties");
    }

    String peerName = commandLine.getOptionValue('p',"hbasekafka");

    boolean createPeer = commandLine.hasOption('a');

    LOG.info("using peer named " + peerName + " automatically create? " + createPeer);

    ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(20000, 20);

    try (CuratorFramework zk = CuratorFrameworkFactory.newClient(zookeeperQ, retryPolicy);
    ) {

      zk.start();

      String basePath = "/hbasekafkaproxy";
      // always gives the same uuid for the same name
      UUID uuid = UUID.nameUUIDFromBytes(Bytes.toBytes(peerName));
      byte []uuidBytes = Bytes.toBytes(uuid.toString());
      String idPath=basePath+"/hbaseid";
      if (zk.checkExists().forPath(idPath) == null) {
        zk.create().creatingParentsIfNeeded().forPath(basePath +
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

      if (zk.checkExists().forPath(basePath + "/rs") == null) {
        zk.create().forPath(basePath + "/rs");
      }

      // look for and connect to the peer
      checkForOrCreateReplicationPeer(conf, zk, basePath, peerName, createPeer);

      Properties configProperties = new Properties();

      if (commandLine.getOptionValue('f') != null){
        try (FileInputStream fs = new java.io.FileInputStream(
                new File(commandLine.getOptionValue('f')))){
          configProperties.load(fs);
        }
      } else {
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
      }

      configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.ByteArraySerializer");
      configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.ByteArraySerializer");
      Producer<byte[], byte[]> producer = new KafkaProducer<>(configProperties);

      ProxyHRegionServer proxy = new ProxyHRegionServer(zk, conf, peerName, producer, routingRules);

      if (autoPeer) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
          @Override
          public void run() {
            LOG.info("***** STOPPING service '" +
                    KafkaProxyServer.class.getSimpleName() + "' *****");
            try (Connection conn = ConnectionFactory.createConnection(conf);
                ReplicationAdmin repAdmin = new ReplicationAdmin(conf)) {
              // disable peer if we added it with the -a flag
              LOG.info("disable peer " + peerName + " since the proxy was "
                  + "started in autopeer mode");
              repAdmin.disablePeer(peerName);
            } catch (Exception e) {
              LOG.error("unable to disable peer " + peerName,e);
            }

            proxy.stop();
            zk.close();
          }
        });
      }

      proxy.start();

      while (true) {
        try {
          Thread.sleep(60000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOG.info("Sleep interrupted",e);
        }
      }
    } catch (Exception e) {
      LOG.error("Exception in proxy",e);
    }
    return 0;
  }

  /**
   * Poll for the configured peer or create it if it does not exist
   *  (controlled by createIfMissing)
   * @param hbaseConf the hbase configuratoin
   * @param zk CuratorFramework object
   * @param basePath base znode.
   * @param internalName id if the peer to check for/create.
   * @param createIfMissing if the peer doesn't exist, create it and peer to it.
   */
  public void checkForOrCreateReplicationPeer(Configuration hbaseConf,
                                              CuratorFramework zk, String basePath,
                                              String peerName, boolean createIfMissing) {
    try (Connection conn = ConnectionFactory.createConnection(hbaseConf);
         Admin admin = conn.getAdmin();
             ReplicationAdmin repAdmin = new ReplicationAdmin(hbaseConf)) {

      boolean peerThere = false;

      while (!peerThere) {
        try {
          ReplicationPeerConfig peerConfig = repAdmin.getPeerConfig(peerName);
          
          if (peerConfig !=null) {
            peerThere=true;
          }
          if ((createIfMissing) && (peerThere)) {
            LOG.info("enable peer," + peerName + " autocreate is set");
            repAdmin.enablePeer(peerName);
          } else if (createIfMissing && !peerThere) {
            ReplicationPeerConfig rconf = new ReplicationPeerConfig();
            // get the current cluster's ZK config
            String zookeeperQ = hbaseConf.get("hbase.zookeeper.quorum") +
                    ":" + hbaseConf.get("hbase.zookeeper.property.clientPort");
            String znodePath = zookeeperQ + ":/hbasekafkaproxy";
            rconf.setClusterKey(znodePath);
            repAdmin.addPeer(peerName, rconf, null);
            repAdmin.enablePeer(peerName);
            peerThere = true;
          }
        } catch (ReplicationException e) {
          LOG.error("Exception attempting to peer",e);
        }

        if (peerThere) {
          break;
        } else {
          LOG.info("peer "+peerName+" not found");
        }
        Thread.sleep(5000);
      }

      LOG.info("found replication peer " + peerName);

    } catch (Exception e) {
      LOG.error("Exception running proxy ",e);
    }
  }
}
