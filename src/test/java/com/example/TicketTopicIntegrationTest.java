package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


class TicketTopicIntegrationTest {

    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String TICKET_TOPIC = "ticket-topic";
    private static final String TICKET_PROCESSED_TOPIC = "ticket-processed-topic";
    private static final boolean VERIFY_PROCESSED_TOPIC = true;
    private static final int MESSAGE_COUNT = 10_000;
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(800);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void shouldClearTicketTopicProduceMessagesPrintCountAndCalculateTps() throws Exception {
        createTopicIfMissing(TICKET_TOPIC, 3);
        if (VERIFY_PROCESSED_TOPIC) {
            createTopicIfMissing(TICKET_PROCESSED_TOPIC, 3);
        }

        purgeTopic(TICKET_TOPIC);
        if (VERIFY_PROCESSED_TOPIC) {
            purgeTopic(TICKET_PROCESSED_TOPIC);
        }

        long countAfterPurge = countTopicMessages(TICKET_TOPIC, "count-after-purge");
        System.out.printf("[STEP-1] topic=%s 清空完成，目前筆數=%d%n", TICKET_TOPIC, countAfterPurge);
        assertEquals(0L, countAfterPurge, "ticket-topic 應該先被清空");

        long produceStartNs = System.nanoTime();
        produceBatch(TICKET_TOPIC, MESSAGE_COUNT);
        long produceEndNs = System.nanoTime();

        long topicCount = waitUntilTopicCountAtLeast(TICKET_TOPIC, MESSAGE_COUNT, Duration.ofSeconds(20));
        double produceSeconds = nanosToSeconds(produceEndNs - produceStartNs);
        double produceTps = MESSAGE_COUNT / produceSeconds;

        System.out.printf("[STEP-2] 已送出 %,d 筆到 %s%n", MESSAGE_COUNT, TICKET_TOPIC);
        System.out.printf("[STEP-3] topic=%s 目前可讀到的筆數=%d%n", TICKET_TOPIC, topicCount);
        System.out.printf("[STEP-4] Produce TPS = %,.2f msg/s (%,d / %.3f 秒)%n", produceTps, MESSAGE_COUNT, produceSeconds);

        assertTrue(topicCount >= MESSAGE_COUNT, "topic 內訊息數應該至少等於送出的筆數");

        if (VERIFY_PROCESSED_TOPIC) {
            long processedStartNs = System.nanoTime();
            long processedCount = waitUntilTopicCountAtLeast(TICKET_PROCESSED_TOPIC, MESSAGE_COUNT, Duration.ofSeconds(60));
            long processedEndNs = System.nanoTime();
            double processedSeconds = nanosToSeconds(processedEndNs - processedStartNs);
            double pipelineTps = processedCount / processedSeconds;

            System.out.printf("[PIPELINE] topic=%s 已處理筆數=%d%n", TICKET_PROCESSED_TOPIC, processedCount);
            System.out.printf("[PIPELINE] End-to-end TPS = %,.2f msg/s (%d / %.3f 秒)%n", pipelineTps, processedCount, processedSeconds);
        } else {
            System.out.println("[NOTE] 目前只量到『寫進 ticket-topic』的 TPS。\n" +
                    "如果你要量 Flink 真正處理 TPS，請先把 app 跑起來，然後把 VERIFY_PROCESSED_TOPIC 改成 true。");
        }
    }

    private static void produceBatch(String topic, int total) throws Exception {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps())) {
            CountDownLatch latch = new CountDownLatch(total);
            AtomicInteger acked = new AtomicInteger();
            AtomicReference<Exception> firstError = new AtomicReference<>();

            for (int i = 0; i < total; i++) {
                String key = "ticket-" + i;
                String value = OBJECT_MAPPER.writeValueAsString(buildTicketEvent(i));

                producer.send(new ProducerRecord<>(topic, key, value), (metadata, exception) -> {
                    try {
                        if (exception != null) {
                            firstError.compareAndSet(null, exception instanceof Exception
                                    ? (Exception) exception
                                    : new RuntimeException(exception));
                            return;
                        }

                        int done = acked.incrementAndGet();
                        if (done % 1_000 == 0) {
                            System.out.printf("[PRODUCER] 已 ack %,d 筆%n", done);
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }

            producer.flush();

            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("producer callback 等待逾時，目前 ack=" + acked.get());
            }
            if (firstError.get() != null) {
                throw firstError.get();
            }
        }
    }

    private static Map<String, Object> buildTicketEvent(int i) {
        long now = System.currentTimeMillis();
        Map<String, Object> event = new HashMap<>();
        event.put("ticketId", "ticket-" + i);
        event.put("eventId", UUID.randomUUID().toString());
        event.put("playerId", "player-" + (i % 100));
        event.put("gameId", "game-" + (i % 20));
        event.put("roundId", "round-" + i);
        event.put("betAmount", BigDecimal.valueOf(10 + (i % 10)));
        event.put("validBetAmount", BigDecimal.valueOf(10 + (i % 10)));
        event.put("winAmount", BigDecimal.valueOf(i % 3));
        event.put("payoutAmount", BigDecimal.valueOf(i % 5));
        event.put("currency", "TWD");
        event.put("status", "SETTLED");
        event.put("platform", "WEB");
        event.put("deviceType", "DESKTOP");
        event.put("eventTime", now);
        event.put("createdAt", now);
        event.put("updatedAt", now);
        return event;
    }

    private static void purgeTopic(String topic) throws Exception {
        try (AdminClient adminClient = AdminClient.create(adminProps())) {
            Collection<TopicPartition> partitions = waitForPartitions(adminClient, topic, Duration.ofSeconds(10));
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> endOffsets;

            try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps("purge-offset-reader-" + topic))) {
                consumer.assign(partitions);
                consumer.seekToEnd(partitions);
                endOffsets = new HashMap<>();
                for (TopicPartition partition : partitions) {
                    endOffsets.put(partition, new org.apache.kafka.clients.consumer.OffsetAndMetadata(consumer.position(partition)));
                }
            }

            Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
            for (Map.Entry<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> entry : endOffsets.entrySet()) {
                recordsToDelete.put(entry.getKey(), RecordsToDelete.beforeOffset(entry.getValue().offset()));
            }

            DeleteRecordsResult result = adminClient.deleteRecords(recordsToDelete);
            result.all().get(10, TimeUnit.SECONDS);
        }
    }

    private static long waitUntilTopicCountAtLeast(String topic, long expected, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        long lastSeen = -1;
        while (System.nanoTime() < deadline) {
            long count = countTopicMessages(topic, "counter-" + topic);
            lastSeen = count;
            if (count >= expected) {
                return count;
            }
            Thread.sleep(500);
        }
        return lastSeen;
    }

    private static long countTopicMessages(String topic, String groupId) throws Exception {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps(groupId + "-" + UUID.randomUUID()))) {
            List<TopicPartition> partitions = fetchPartitions(consumer, topic, Duration.ofSeconds(10));
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            long total = 0;
            int emptyPolls = 0;
            while (emptyPolls < 3) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                if (records.isEmpty()) {
                    emptyPolls++;
                    continue;
                }

                emptyPolls = 0;
                total += records.count();

                for (ConsumerRecord<String, String> record : records) {
                    if (total <= 5) {
                        System.out.printf("[MONITOR] topic=%s partition=%d offset=%d key=%s%n",
                                topic, record.partition(), record.offset(), record.key());
                    }
                }
            }
            return total;
        }
    }

    private static void createTopicIfMissing(String topic, int partitions) throws Exception {
        try (AdminClient adminClient = AdminClient.create(adminProps())) {
            Set<String> topics = adminClient.listTopics().names().get(10, TimeUnit.SECONDS);
            if (topics.contains(topic)) {
                return;
            }
            adminClient.createTopics(Collections.singletonList(new NewTopic(topic, partitions, (short) 1)))
                    .all().get(10, TimeUnit.SECONDS);
        }
    }

    private static Collection<TopicPartition> waitForPartitions(AdminClient adminClient, String topic, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps("partition-check-" + UUID.randomUUID()))) {
                List<TopicPartition> partitions = fetchPartitions(consumer, topic, Duration.ofSeconds(3));
                if (!partitions.isEmpty()) {
                    return partitions;
                }
            }
            Thread.sleep(300);
        }
        throw new IllegalStateException("找不到 topic partitions: " + topic);
    }

    private static List<TopicPartition> fetchPartitions(KafkaConsumer<String, String> consumer, String topic, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            var partitionInfos = consumer.partitionsFor(topic, Duration.ofMillis(500));
            if (partitionInfos != null && !partitionInfos.isEmpty()) {
                List<TopicPartition> partitions = new ArrayList<>();
                partitionInfos.forEach(p -> partitions.add(new TopicPartition(topic, p.partition())));
                return partitions;
            }
        }
        throw new IllegalStateException("topic 不存在或 partitions 尚未就緒: " + topic);
    }

    private static Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(64 * 1024));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        return props;
    }

    private static Properties consumerProps(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2000");
        return props;
    }

    private static Properties adminProps() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        return props;
    }

    private static double nanosToSeconds(long nanos) {
        return nanos / 1_000_000_000.0;
    }
}
