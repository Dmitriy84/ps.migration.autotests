package com.adidas.utils.kafka;


import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {
    public static void template (){
        Properties props = new Properties();

        props.put("bootstrap.servers", "kafka1:9093");
        props.put("group.id", "test");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("security.protocol", "SSL");
        props.put("ssl.enabled.protocols", "TLSv1.2,TLSv1.1,TLSv1");
        props.put("ssl.truststore.type", "JKS");
        props.put("ssl.truststore.location", "kafka.truststore.jks");
        props.put("ssl.truststore.password", "CHANGEME");
        props.put("ssl.keystore.type", "JKS");
        props.put("ssl.keystore.location", "kafka.keystore.jks");
        props.put("ssl.keystore.password", "CHANGEME");
        props.put("ssl.key.password", "CHANGEME");

        final KafkaProducer producer = new KafkaProducer<>(props);
        for (int i = 0; i < 1000; i++){
            ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", Integer.toString(i));
            producer.send(record, (metadata, e) -> {
                if (e != null) {
                    e.printStackTrace();
                } else {
                    System.out.println("offset: " + metadata.offset());
                }
            });
        }
        producer.close();
    }
}
