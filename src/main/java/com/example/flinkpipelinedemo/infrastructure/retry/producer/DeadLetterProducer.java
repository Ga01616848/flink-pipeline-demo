package com.example.flinkpipelinedemo.infrastructure.retry.producer;

import com.example.flinkpipelinedemo.infrastructure.sink.KafkaProducerManager;
import com.example.flinkpipelinedemo.infrastructure.retry.model.RetryEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class DeadLetterProducer {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void sendToDeadLetterTopic(RetryEvent retryEvent) {
        KafkaProducer<String, String> producer = KafkaProducerManager.getProducer();

        try {
            String json = objectMapper.writeValueAsString(retryEvent);
            System.out.println("Trying to Send to dead-letter-topic: " + json);

            RecordMetadata metadata =
                    producer.send(new ProducerRecord<>("dead-letter-topic", json)).get();

            producer.flush();

            System.out.println("Dead-letter-topic received, partition=" + metadata.partition()
                    + ", offset=" + metadata.offset());

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed sending to dead-letter-topic ", e);
        } finally {
        }
    }
}