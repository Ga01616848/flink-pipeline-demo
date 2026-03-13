package src.main.java.com.example.tool;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class TicketProducer {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = "localhost:29092";
        String topic = "ticket-topic";
        int totalMessages = 100_000;
        int throughputPerSecond = 100_000;

        String clickhouseUrl = "jdbc:clickhouse://localhost:8123/default?compress=false";
        String clickhouseUser = "default";
        String clickhousePassword = "password123";

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "1");
        props.put("linger.ms", "5");
        props.put("batch.size", "65536");
        props.put("compression.type", "lz4");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        long start = System.currentTimeMillis();

        for (int i = 1; i <= totalMessages; i++) {
            String ticketId = "t" + i;
            long eventTime = System.currentTimeMillis();

            String json = "{"
                    + "\"ticketId\":\"" + ticketId + "\","
                    + "\"playerId\":\"p" + (i % 100000) + "\","
                    + "\"gameId\":\"g001\","
                    + "\"roundId\":\"r001\","
                    + "\"betAmount\":100.00,"
                    + "\"winAmount\":0.00,"
                    + "\"currency\":\"USD\","
                    + "\"status\":\"SETTLED\","
                    + "\"eventTime\":" + eventTime
                    + "}";

            producer.send(new ProducerRecord<>(topic, ticketId, json));

            if (i % throughputPerSecond == 0) {
                long elapsed = System.currentTimeMillis() - start;
                long expectedElapsed = (i / throughputPerSecond) * 1000L;
                if (expectedElapsed > elapsed) {
                    Thread.sleep(expectedElapsed - elapsed);
                }
                System.out.println("sent=" + i);
            }
        }

        producer.flush();
        producer.close();

        long sendDoneAt = System.currentTimeMillis();
        double actualTps = totalMessages * 1000.0 / (sendDoneAt - start);
        System.out.println("Kafka send done. actualTPS=" + actualTps);

        long deadline = sendDoneAt + 3000;
        long currentCount = 0;

        try (Connection conn = DriverManager.getConnection(clickhouseUrl, clickhouseUser, clickhousePassword);
             Statement stmt = conn.createStatement()) {

            while (System.currentTimeMillis() < deadline) {
                try (ResultSet rs = stmt.executeQuery("SELECT count() FROM ticket_data")) {
                    if (rs.next()) {
                        currentCount = rs.getLong(1);
                    }
                }

                System.out.println("Current ClickHouse count=" + currentCount);

                if (currentCount >= totalMessages) {
                    System.out.println("SUCCESS: ClickHouse caught up within 3 seconds.");
                    return;
                }

                Thread.sleep(200);
            }
        }

        System.out.println("FAILED: ClickHouse did not catch up within 3 seconds. Final count=" + currentCount);
    }
}