package org.hbasejanitor.hbase.kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.hbasejanitor.hbase.event.HBase10Event;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

/**
 * Provide an out of the box service that can read avro encoded objects produced by the kafka replication proxy and dump them into hbase.
 */
public class KafkaToHbaseConnector extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(KafkaProxyServer.class);

  
  public static void main(String[] args) {
    try {
      ToolRunner.run(new KafkaToHbaseConnector(), args);
    } catch (Exception e) {
      LOG.error("Exception running proxy ",e);
    }
  }
  
  
  private static void printUsageAndExit(Options options, int exitCode) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("hbase org.hbasejanitor.hbase.kafka.KafkaToHbaseConnector", "", options,"", true);
    System.exit(exitCode);
  }

  @Override
  public int run(String[] args) {
    LOG.info("***** STARTING service '" + KafkaToHbaseConnector.class.getSimpleName() + "' *****");
    VersionInfo.logVersion();

    Options options = new Options();

    options.addOption("b", "kafkabrokers", true,
            "Kafka Brokers (comma delimited)");
    options.addOption("f", "kafkaproperties", false,
            "Path to properties file that has the kafka connection properties");

    Option opt = new Option("t", "topics", true, "topics to listen on (comma delimited)");
    opt.setRequired(true);
    options.addOption(opt);
    
    opt = new Option("c", "consumergroup", true, "Consumer group to use");
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

    Properties configProperties = new Properties();
    String kafkaServers = commandLine.getOptionValue('b');

    String topic= commandLine.getOptionValue('t');
    String consumergroup= commandLine.getOptionValue('c');
    
    if (commandLine.getOptionValue('f') != null){
      try (FileInputStream fs = new java.io.FileInputStream(
              new File(commandLine.getOptionValue('f')))){
        configProperties.load(fs);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
    }

    configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    configProperties.put("group.id", consumergroup);

    
    SpecificDatumReader<HBase10Event> dreader =
        new SpecificDatumReader<>(HBase10Event.SCHEMA$);

    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(configProperties);
        Connection hbaseConnect = ConnectionFactory.createConnection()){
      consumer.subscribe(Arrays.stream(topic.split(",")).collect(Collectors.toList()));
      
      Runtime.getRuntime().addShutdownHook(new Thread(()-> {
        consumer.close();
      }));

      Cache<String, Table> cache = Caffeine.newBuilder()
          .maximumSize(20)
          .removalListener(KafkaToHbaseConnector::closeTable)
          .build();
      
      while (true) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(10000);
        Iterator<ConsumerRecord<byte[], byte[]>> it = records.iterator();
        while (it.hasNext()) {
          ConsumerRecord<byte[], byte[]> record = it.next();
          BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(record.value(), null);
          try {
            HBase10Event event = dreader.read(null, decoder);

            String tableName=event.getTable().duplicate().asCharBuffer().toString();
            Table hbaseTable = cache.get(tableName, (stringTableName) -> {
              try {
                return hbaseConnect.getTable(TableName.valueOf(stringTableName));  
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

            if (event.getDelete()) {
              Delete delete = new Delete(event.getKey().array(),event.getKey().position(),event.getKey().limit(),event.getTimestamp());
              byte family[] = new byte[event.getFamily().limit()-event.getFamily().position()];
              byte qualifier[] = new byte[event.getQualifier().limit()-event.getQualifier().position()];
              event.getFamily().duplicate().get(family);
              event.getQualifier().duplicate().get(qualifier);
              delete.addColumn(family, qualifier,event.getTimestamp());
              hbaseTable.delete(delete);
            } else {
              Put myPut = new Put(event.getKey().duplicate());
              byte family[] = new byte[event.getFamily().limit()-event.getFamily().position()];
              event.getFamily().duplicate().get(family);
              myPut.addImmutable(family, event.getQualifier().duplicate(), event.getTimestamp(), event.getValue().duplicate());
              hbaseTable.put(myPut);
            }
            
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  } 
  
  public static void closeTable(String key, Table table, RemovalCause why) {
    try {
      table.close();  
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
}
