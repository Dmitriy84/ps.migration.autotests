package com.adidas.utils.kafka;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Consumer {

    public static final String OFFSET_KEY_VALUE= "offset = %d, key = %s, value = %s%n";

    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("bootstrap.servers", "kafka1:9093");
        props.put("group.id", "test-consumer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "10000");
        props.put("max.poll.records", "10");
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("security.protocol", "SSL");
        props.put("ssl.enabled.protocols", "TLSv1.2,TLSv1.1,TLSv1");
        props.put("ssl.truststore.type", "JKS");
        props.put("ssl.truststore.location", "kafka.truststore.jks");
        props.put("ssl.truststore.password", "CHANGEME");
        props.put("ssl.keystore.type", "JKS");
        props.put("ssl.keystore.location", "kafka.keystore.jks");
        props.put("ssl.keystore.password", "CHANGEME");
        props.put("ssl.key.password", "CHANGEME");

        ConsumerRecords<String, String> records;
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test-topic"), new ConsumerRebalanceListener() {

            @Override public void onPartitionsRevoked(final Collection<TopicPartition> collection) {
                System.out.println("Partitions Revoked");
                collection.forEach(partition -> System.out.println(partition.toString()));
            }

            @Override public void onPartitionsAssigned(final Collection<TopicPartition> collection) {
                System.out.println("Partitions Assigned");
                collection.forEach(partition -> System.out.println(partition.toString()));
            }
        });
        consumer.poll(10);
        consumer.seekToBeginning(consumer.assignment());
        do {
            records = consumer.poll(1000);
            records.forEach(record -> System.out.printf(OFFSET_KEY_VALUE, record.offset(), record.key(), record.value()));
        } while (!records.isEmpty());
        consumer.close();
    }
}