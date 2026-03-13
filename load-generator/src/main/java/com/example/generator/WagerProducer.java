package main.java.com.example.generator;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class WagerProducer {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("batch.size", 32768);
        props.put("linger.ms", 5);
        props.put("compression.type", "lz4");

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(props);

        int total = 100_000_000;

        for (int i = 0; i < total; i++) {

            String json = generateEvent(i);

            producer.send(
                    new ProducerRecord<>("ticket-topic", json)
            );

            if (i % 10000 == 0) {
                System.out.println("sent " + i);
            }
        }

        producer.flush();
        producer.close();
    }

    private static String generateEvent(int i) {

        return "{"
                + "\"ticketId\":\"" + i + "\","
                + "\"playerId\":\"p" + (i % 100000) + "\","
                + "\"gameId\":\"g1\","
                + "\"betAmount\":10,"
                + "\"winAmount\":0,"
                + "\"currency\":\"USD\","
                + "\"status\":\"BET\","
                + "\"eventTime\":" + System.currentTimeMillis()
                + "}";
    }
}