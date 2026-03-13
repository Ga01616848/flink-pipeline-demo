package com.example.flinkpipelinedemo.infrastructure.retry.job;

import com.example.flinkpipelinedemo.infrastructure.clickhouse.config.ClickHouseConfig;
import com.example.flinkpipelinedemo.infrastructure.clickhouse.sink.ClickHouseSinkConfig;
import com.example.flinkpipelinedemo.infrastructure.retry.model.RetryEvent;
import com.example.flinkpipelinedemo.infrastructure.retry.producer.DeadLetterProducer;
import com.example.flinkpipelinedemo.infrastructure.sink.KafkaProducerManager;
import com.example.flinkpipelinedemo.shared.config.AppProperties;
import com.example.flinkpipelinedemo.shared.util.JsonUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.LocalDateTime;

@Component
public class RetryPipeline {

    private final AppProperties props;

    public RetryPipeline(AppProperties props) {
        this.props = props;
    }

    public void build(StreamExecutionEnvironment env) {
        String bootstrapServers = props.getKafka().getBootstrapServers();
        String retryTopic = props.getKafka().getRetryTopic();
        String retryGroupId = props.getKafka().getRetryGroupId();
        int maxRetryCount = props.getRetry().getMaxRetryCount();
        long retryBackoffMs = props.getRetry().getRetryBackoffMs();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(retryTopic)
                .setGroupId(retryGroupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "retry-kafka-source"
        );

        DataStream<RetryEvent> retryEventStream = rawStream
                .flatMap(new RetryEventParser());

        // retry
        DataStream<RetryEvent> retryableStream = retryEventStream
                .filter(event -> {
                    int retryCount = event.getRetryCount() == null ? 0 : event.getRetryCount();
                    return retryCount < maxRetryCount;
                });

        // dead-letter-topic
        DataStream<RetryEvent> deadLetterStream = retryEventStream
                .filter(event -> {
                    int retryCount = event.getRetryCount() == null ? 0 : event.getRetryCount();
                    return retryCount >= maxRetryCount;
                });

        retryableStream
                .map(new RetryToSourceProcessor(retryBackoffMs))
                .name("retry-to-source-processor")
                .print("RETRY_TO_SOURCE");

        deadLetterStream
                .map(new DeadLetterSender())
                .name("dead-letter-sender")
                .print("SEND_TO_DLQ");

        deadLetterStream.addSink(
                org.apache.flink.connector.jdbc.JdbcSink.sink(
                        "INSERT INTO pipeline_errors (topic, raw_data, error_message, retry_count, created_at) VALUES (?, ?, ?, ?, ?)",
                        (statement, event) -> {
                            statement.setString(1, defaultString(event.getSourceTopic(), "UNKNOWN"));
                            statement.setString(2, defaultString(event.getRawData(), ""));
                            statement.setString(3, defaultString(event.getErrorMessage(), "UNKNOWN_ERROR"));
                            statement.setInt(4, event.getRetryCount() == null ? 0 : event.getRetryCount());
                            statement.setTimestamp(5, Timestamp.valueOf(LocalDateTime.now()));
                        },
                        ClickHouseSinkConfig.getExecutionOptions(),
                        ClickHouseConfig.getOptions(props)
                )
        );
    }

    public static class RetryEventParser extends RichMapFunction<String, RetryEvent>
            implements FlatMapFunction<String, RetryEvent> {

        @Override
        public RetryEvent map(String value) {
            return parseOrNull(value);
        }

        @Override
        public void flatMap(String value, Collector<RetryEvent> out) {
            RetryEvent event = parseOrNull(value);
            if (event != null) {
                out.collect(event);
            }
        }

        private RetryEvent parseOrNull(String value) {
            System.out.println("[RetryEventParser] raw=" + value);

            try {
                RetryEvent event = JsonUtils.parse(value, RetryEvent.class);

                if (event == null) {
                    System.out.println("[RetryEventParser] parsed event is null, raw=" + value);
                    sendBadMessageToDeadLetter(value, "parsed event is null");
                    return null;
                }

                if (isBlank(event.getSourceTopic()) || isBlank(event.getRawData())) {
                    System.out.println("[RetryEventParser] invalid event, sourceTopic/rawData missing, event=" + safeToJson(event));
                    sendEventToDeadLetterSafely(event, "sourceTopic or rawData is blank");
                    return null;
                }

                if (event.getRetryCount() == null) {
                    event.setRetryCount(0);
                }

                return event;

            } catch (Exception e) {
                System.out.println("[RetryEventParser] parse failed, raw=" + value + ", error=" + e.getMessage());
                sendBadMessageToDeadLetter(value, e.getMessage());
                return null;
            }
        }

        private void sendBadMessageToDeadLetter(String rawValue, String reason) {
            try {
                RetryEvent badEvent = new RetryEvent();
                badEvent.setSourceTopic("UNKNOWN");
                badEvent.setRawData(rawValue);
                badEvent.setErrorMessage(reason);
                badEvent.setRetryCount(0);

                DeadLetterProducer.sendToDeadLetterTopic(badEvent);
                System.out.println("[RetryEventParser] bad message sent to DLQ, reason=" + reason);
            } catch (Exception ex) {
                System.out.println("[RetryEventParser] failed to send bad message to DLQ, error=" + ex.getMessage());
            }
        }

        private void sendEventToDeadLetterSafely(RetryEvent event, String reason) {
            try {
                event.setErrorMessage(reason);
                DeadLetterProducer.sendToDeadLetterTopic(event);
                System.out.println("[RetryEventParser] invalid event sent to DLQ, reason=" + reason);
            } catch (Exception ex) {
                System.out.println("[RetryEventParser] failed to send invalid event to DLQ, error=" + ex.getMessage());
            }
        }
    }

    public static class RetryToSourceProcessor extends RichMapFunction<RetryEvent, String> {

        private final long retryBackoffMs;
        private transient KafkaProducer<String, String> producer;

        public RetryToSourceProcessor(long retryBackoffMs) {
            this.retryBackoffMs = retryBackoffMs;
        }

        @Override
        public void open(Configuration parameters) {
            this.producer = KafkaProducerManager.getProducer();
        }

        @Override
        public String map(RetryEvent event) throws Exception {
            validateEvent(event);

            if (retryBackoffMs > 0) {
                Thread.sleep(retryBackoffMs);
            }

            String sourceTopic = event.getSourceTopic();
            String payload = JsonUtils.toJson(event);

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(sourceTopic, payload);

            producer.send(record).get();
            producer.flush();

            String result = "[RetryToSourceProcessor] sent back to source topic"
                    + ", sourceTopic=" + event.getSourceTopic()
                    + ", retryCount=" + event.getRetryCount()
                    + ", rawData=" + event.getRawData();

            System.out.println(result);
            return result;
        }
    }

    public static class DeadLetterSender extends RichMapFunction<RetryEvent, String> {

        @Override
        public String map(RetryEvent event) {
            try {
                validateEvent(event);

                DeadLetterProducer.sendToDeadLetterTopic(event);

                String result = "[DeadLetterSender] sent to dead-letter-topic"
                        + ", sourceTopic=" + event.getSourceTopic()
                        + ", retryCount=" + event.getRetryCount()
                        + ", rawData=" + event.getRawData();

                System.out.println(result);
                return result;

            } catch (Exception e) {
                String result = "[DeadLetterSender] failed to send dead-letter-topic"
                        + ", sourceTopic=" + (event == null ? "null" : event.getSourceTopic())
                        + ", retryCount=" + (event == null ? "null" : event.getRetryCount())
                        + ", rawData=" + (event == null ? "null" : event.getRawData())
                        + ", error=" + e.getMessage();

                System.out.println(result);
                return result;
            }
        }
    }

    private static void validateEvent(RetryEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("event is null");
        }
        if (isBlank(event.getSourceTopic())) {
            throw new IllegalArgumentException("sourceTopic is null or blank");
        }
        if (isBlank(event.getRawData())) {
            throw new IllegalArgumentException("rawData is null or blank");
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static String safeToJson(Object obj) {
        try {
            return JsonUtils.toJson(obj);
        } catch (Exception e) {
            return String.valueOf(obj);
        }
    }

    private static String defaultString(String value, String defaultValue) {
        return isBlank(value) ? defaultValue : value;
    }
}