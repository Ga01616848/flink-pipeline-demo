package com.example.flinkpipelinedemo.infrastructure.retry.producer;

import com.example.flinkpipelinedemo.infrastructure.sink.KafkaProducerManager;
import com.example.flinkpipelinedemo.infrastructure.retry.model.RetryEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class RetryProducer {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void sendToRetryTopic(RetryEvent retryEvent) {
        KafkaProducer<String, String> producer = KafkaProducerManager.getProducer();

        try {
            String json = objectMapper.writeValueAsString(retryEvent);
            System.out.println("Trying to Send to retry-topic: " + json);

            RecordMetadata metadata = producer.send(new ProducerRecord<>("retry-topic", json)).get();
            producer.flush();

            System.out.println("Retry-topic received, partition=" + metadata.partition()
                    + ", offset=" + metadata.offset());

        } catch (Exception e) {
            System.out.println("Failed sending to retry-topic ");
            e.printStackTrace();
        }
    }
}