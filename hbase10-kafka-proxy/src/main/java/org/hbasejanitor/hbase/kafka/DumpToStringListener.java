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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.hbasejanitor.hbase.event.HBase10Event;



/**
 * connects to kafka and reads from the passed in topics.  Parses each message into an avro object
 * and dumps it to the console.
 */
public final class DumpToStringListener {
  private static final Log LOG = LogFactory.getLog(DumpToStringListener.class);

  private DumpToStringListener(){

  }

  public static void main(String[] args) {
    LOG.info("***** STARTING service '" + DumpToStringListener.class.getSimpleName() + "' *****");
    VersionInfo.logVersion();

    Options options = new Options();
    options.addOption("k", "kafkabrokers", true, "Kafka Brokers " +
            "(comma delimited)");
    options.addOption("t", "kafkatopics", true,"Kafka Topics "
        + "to subscribe to (comma delimited)");
    CommandLine commandLine = null;
    try {
      commandLine = new PosixParser().parse(options, args);
      
    } catch (ParseException e) {
      LOG.error("Could not parse: ", e);
      printUsageAndExit(options, -1);
    }
    SpecificDatumReader<HBase10Event> dreader =
            new SpecificDatumReader<>(HBase10Event.SCHEMA$);

    String topic = commandLine.getOptionValue('t');
    Properties props = new Properties();
    props.put("bootstrap.servers", commandLine.getOptionValue('k'));
    props.put("group.id", "hbase kafka test tool");
    props.put("key.deserializer", ByteArrayDeserializer.class.getName());
    props.put("value.deserializer", ByteArrayDeserializer.class.getName());

    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);){
      consumer.subscribe(Arrays.stream(topic.split(",")).collect(Collectors.toList()));

      while (true) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(10000);
        Iterator<ConsumerRecord<byte[], byte[]>> it = records.iterator();
        while (it.hasNext()) {
          ConsumerRecord<byte[], byte[]> record = it.next();
          BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(record.value(), null);
          try {
            HBase10Event event = dreader.read(null, decoder);
            LOG.info("key :" + Bytes.toString(record.key()) + " value " + event);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
  }

  private static void printUsageAndExit(Options options, int exitCode) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("hbase " + DumpToStringListener.class.getName(), "", options,
            "\n[--kafkabrokers <kafka brokers (comma delmited)>] " +
                    "[-k <kafka brokers (comma delmited)>] \n", true);
    System.exit(exitCode);
  }

}
