package com.example.flinkpipelinedemo.infrastructure.sink;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public class KafkaProducerManager {

    private static KafkaProducer<String, String> producer;

    public static KafkaProducer<String, String> getProducer() {
        if (producer == null) {
            synchronized (KafkaProducerManager.class) {
                if (producer == null) {
                    Properties props = new Properties();
                    props.put("bootstrap.servers", "localhost:29092");
                    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    props.put("acks", "all");
                    props.put("retries", "3");
                    props.put("batch.size", "16384");
                    props.put("linger.ms", "5");
                    props.put("buffer.memory", "33554432");

                    producer = new KafkaProducer<>(props);
                }
            }
        }
        return producer;
    }
}