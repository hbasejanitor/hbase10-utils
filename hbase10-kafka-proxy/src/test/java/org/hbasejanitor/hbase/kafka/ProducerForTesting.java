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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.hbasejanitor.hbase.event.HBase10Event;

/**
 * Mocks Kafka producer for testing
 */
public class ProducerForTesting implements Producer<byte[], byte[]> {
  Map<String, List<HBase10Event>> messages = new HashMap<>();
  SpecificDatumReader<HBase10Event> dreader = new SpecificDatumReader<>(HBase10Event.SCHEMA$);

  public Map<String, List<HBase10Event>> getMessages() {
    return messages;
  }

  @Override
  public void abortTransaction() throws ProducerFencedException {
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> producerRecord) {
    try {

      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(producerRecord.value(), null);
      HBase10Event event = dreader.read(null, decoder);
      if (!messages.containsKey(producerRecord.topic())) {
        messages.put(producerRecord.topic(), new ArrayList<>());
      }
      messages.get(producerRecord.topic()).add(event);
      return new Future<RecordMetadata>() {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
          // TODO Auto-generated method stub
          return false;
        }

        @Override
        public boolean isCancelled() {
          // TODO Auto-generated method stub
          return false;
        }

        @Override
        public boolean isDone() {
          // TODO Auto-generated method stub
          return false;
        }

        @Override
        public RecordMetadata get() throws InterruptedException, ExecutionException {
          return new RecordMetadata(null, 1, 1, 1, 1, 1, 1);
        }

        @Override
        public RecordMetadata get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
          // TODO Auto-generated method stub
          return null;
        }
        
      };
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> producerRecord,
      Callback callback) {
    return null;
  }

  @Override
  public void flush() {
  }

  @Override
  public List<PartitionInfo> partitionsFor(String s) {
    return null;
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return null;
  }

  @Override
  public void close() {
  }

  @Override
  public void close(long l, TimeUnit timeUnit) {
  }

  @Override
  public void initTransactions() {
  }

  @Override
  public void beginTransaction() throws ProducerFencedException {
  }

  @Override
  public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
      String consumerGroupId) throws ProducerFencedException {
  }

  @Override
  public void commitTransaction() throws ProducerFencedException {
  }
}
