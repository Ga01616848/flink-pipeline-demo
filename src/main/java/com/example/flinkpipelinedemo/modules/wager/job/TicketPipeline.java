package com.example.flinkpipelinedemo.modules.wager.job;

import com.example.flinkpipelinedemo.application.pipeline.ticket.dto.TicketProcessedEvent;
import com.example.flinkpipelinedemo.infrastructure.retry.model.RetryEvent;
import com.example.flinkpipelinedemo.modules.wager.dedup.WagerIdempotencyRepository;
import com.example.flinkpipelinedemo.modules.wager.enums.TicketStatus;
import com.example.flinkpipelinedemo.modules.wager.exception.WagerProcessException;
import com.example.flinkpipelinedemo.modules.wager.model.TicketEvent;
import com.example.flinkpipelinedemo.modules.wager.service.WagerIdempotencyKeyResolver;
import com.example.flinkpipelinedemo.modules.wager.service.WagerProcessResult;
import com.example.flinkpipelinedemo.modules.wager.service.WagerProcessingService;
import com.example.flinkpipelinedemo.modules.wager.sink.WagerClickHouseGateway;
import com.example.flinkpipelinedemo.shared.config.AppProperties;
import com.example.flinkpipelinedemo.shared.util.JsonUtils;
import com.example.flinkpipelinedemo.modules.wager.sink.JdbcWagerClickHouseGateway;
import com.example.flinkpipelinedemo.modules.wager.dedup.RedisWagerIdempotencyRepository;
import java.time.Duration;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Component
public class TicketPipeline {

    private static final int PIPELINE_PARALLELISM = 8;
    private final AppProperties props;

    public TicketPipeline(AppProperties props) {
        this.props = props;
    }

    private static final OutputTag<RetryEvent> RETRY_OUTPUT_TAG =
            new OutputTag<>("ticket-retry-output") {};

    public void build(StreamExecutionEnvironment env) {
        String bootstrapServers = props.getKafka().getBootstrapServers();
        String ticketTopic = props.getKafka().getTicketTopic();
        String ticketProcessedTopic = props.getKafka().getTicketProcessedTopic();
        String ticketGroupId = props.getKafka().getTicketGroupId();
        String retryTopic = props.getKafka().getRetryTopic();


        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(ticketTopic)
            .setGroupId(ticketGroupId)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> stream =
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "Ticket Kafka Source")
                .setParallelism(PIPELINE_PARALLELISM);

        SingleOutputStreamOperator<TicketEvent> ticketStream = stream
            .process(new TicketEventProcessFunction(ticketTopic, RETRY_OUTPUT_TAG))
            .name("ticket-parse-and-validate")
            .setParallelism(PIPELINE_PARALLELISM);

        DataStream<TicketEvent> enrichedTicketStream = AsyncDataStream.unorderedWait(
                ticketStream,
                new UpdatePlayerTotalBetRedisAsyncFunction(
                    props.getRedis().getHost(),
                    props.getRedis().getPort()
                ),
                3,
                TimeUnit.SECONDS,
                1000
            ).name("update-player-total-bet-redis-async")
            .setParallelism(PIPELINE_PARALLELISM);

        KafkaSink<String> processedKafkaSink = KafkaSink.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(ticketProcessedTopic)
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build()
            )
            .setDeliveryGuarantee(DeliveryGuarantee.NONE)
            .setKafkaProducerConfig(getProducerProps())
            .build();

        KafkaSink<String> retryKafkaSink = KafkaSink.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(retryTopic)
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build()
            )
            .setDeliveryGuarantee(DeliveryGuarantee.NONE)
            .setKafkaProducerConfig(getProducerProps())
            .build();

        DataStream<TicketEvent> wagerHandledStream = enrichedTicketStream
            .map(new WagerProcessMapFunction(
                props.getClickhouse().getUrl(),
                props.getClickhouse().getUsername(),
                props.getClickhouse().getPassword(),
                props.getRedis().getHost(),
                props.getRedis().getPort()
            ))
            .name("wager-clickhouse-idempotent-process")
            .setParallelism(PIPELINE_PARALLELISM);

        wagerHandledStream
            .map(TicketPipeline::toProcessedJson)
            .setParallelism(PIPELINE_PARALLELISM)
            .sinkTo(processedKafkaSink)
            .name("ticket-processed-kafka-sink");

        ticketStream
            .getSideOutput(RETRY_OUTPUT_TAG)
            .map(JsonUtils::toJson)
            .setParallelism(PIPELINE_PARALLELISM)
            .sinkTo(retryKafkaSink)
            .name("ticket-retry-kafka-sink");
    }

    public static class TicketEventProcessFunction extends ProcessFunction<String, TicketEvent> {
        private final String sourceTopic;
        private final OutputTag<RetryEvent> retryOutputTag;
        private transient Counter receivedCounter;
        private transient Counter validCounter;
        private transient Counter retryCounter;

        public TicketEventProcessFunction(String sourceTopic, OutputTag<RetryEvent> retryOutputTag) {
            this.sourceTopic = sourceTopic;
            this.retryOutputTag = retryOutputTag;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            MetricGroup metricGroup = getRuntimeContext().getMetricGroup();

            receivedCounter = metricGroup.counter("ticket_received_total");
            validCounter = metricGroup.counter("ticket_valid_total");
            retryCounter = metricGroup.counter("ticket_retry_total");
        }

        @Override
        public void processElement(String value, Context ctx, Collector<TicketEvent> out) {

            receivedCounter.inc();

            try {
                TicketEvent event = JsonUtils.parse(value, TicketEvent.class);

                String validationError = validate(event);
                if (validationError != null) {
                    retryCounter.inc();
                    ctx.output(retryOutputTag, buildRetryEvent(value, validationError, 0));
                    return;
                }

                validCounter.inc();
                out.collect(event);

            } catch (Exception e) {
                retryCounter.inc();
                ctx.output(retryOutputTag, buildRetryEvent(value, e.getMessage(), 0));
            }
        }

        private RetryEvent buildRetryEvent(String rawData, String errorMessage, int retryCount) {
            RetryEvent retryEvent = new RetryEvent();
            retryEvent.setSourceTopic(sourceTopic);
            retryEvent.setRawData(rawData);
            retryEvent.setErrorMessage(errorMessage);
            retryEvent.setRetryCount(retryCount + 1);
            return retryEvent;
        }

        private String validate(TicketEvent event) {
            if (!hasRequiredFields(event)) {
                return "missing required fields";
            }

            if (!isValidAmount(event)) {
                return "invalid amount";
            }

            if (!isValidCurrency(event.getCurrency())) {
                return "invalid currency";
            }

            if (!isValidStatus(event.getStatus())) {
                return "invalid status";
            }

            if (!isValidEventTime(event.getEventTime())) {
                return "invalid eventTime";
            }

            return null;
        }

        private boolean hasRequiredFields(TicketEvent event) {
            return event != null
                    && isNotBlank(event.getTicketId())
                    && isNotBlank(event.getPlayerId())
                    && isNotBlank(event.getGameId())
                    && isNotBlank(event.getRoundId())
                    && event.getBetAmount() != null
                    && event.getWinAmount() != null
                    && isNotBlank(event.getCurrency())
                    && isNotBlank(event.getStatus())
                    && event.getEventTime() != null;
        }

        private boolean isValidAmount(TicketEvent event) {
            return event.getBetAmount().compareTo(BigDecimal.ZERO) >= 0
                    && event.getWinAmount().compareTo(BigDecimal.ZERO) >= 0;
        }

        private boolean isValidCurrency(String currency) {
            if (currency == null || currency.isBlank()) {
                return false;
            }

            String normalized = currency.trim().toUpperCase();
            return normalized.equals("TWD") || normalized.equals("USD");
        }

        private boolean isValidStatus(String status) {
            return TicketStatus.isValid(status);
        }

        private boolean isValidEventTime(Long eventTime) {
            if (eventTime == null || eventTime <= 0) {
                return false;
            }

            long now = System.currentTimeMillis();
            long maxAllowedFuture = now + 5 * 60 * 1000L;

            return eventTime <= maxAllowedFuture;
        }

        private boolean isNotBlank(String value) {
            return value != null && !value.trim().isEmpty();
        }
    }

    public static class UpdatePlayerTotalBetRedisFunction extends RichMapFunction<TicketEvent, TicketEvent> {
        private final String redisHost;
        private final int redisPort;

        private transient StringRedisTemplate stringRedisTemplate;

        public UpdatePlayerTotalBetRedisFunction(String redisHost, int redisPort) {
            this.redisHost = redisHost;
            this.redisPort = redisPort;
        }

        @Override
        public void open(Configuration parameters) {
            LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(redisHost, redisPort);
            connectionFactory.afterPropertiesSet();

            this.stringRedisTemplate = new StringRedisTemplate();
            this.stringRedisTemplate.setConnectionFactory(connectionFactory);
            this.stringRedisTemplate.afterPropertiesSet();
        }

        @Override
        public TicketEvent map(TicketEvent event) {
            String key = "player:total_bet:" + event.getPlayerId();
            stringRedisTemplate.opsForValue().increment(key, event.getBetAmount().doubleValue());
            return event;
        }
    }

    public static class TicketEventToStringMapper implements MapFunction<TicketEvent, String> {
        @Override
        public String map(TicketEvent event) {
            return "ticketId=" + event.getTicketId()
                    + ", playerId=" + event.getPlayerId()
                    + ", gameId=" + event.getGameId()
                    + ", roundId=" + event.getRoundId()
                    + ", betAmount=" + event.getBetAmount()
                    + ", winAmount=" + event.getWinAmount()
                    + ", currency=" + event.getCurrency()
                    + ", status=" + event.getStatus()
                    + ", eventTime=" + event.getEventTime();
        }
    }

    private Properties getProducerProps() {
        Properties props = new Properties();

        props.setProperty("acks", "1");
        props.setProperty("batch.size", "131072");
        props.setProperty("linger.ms", "20");
        props.setProperty("compression.type", "lz4");
        props.setProperty("buffer.memory", "67108864");
        props.setProperty("max.in.flight.requests.per.connection", "5");

        return props;
    }

    private static TicketProcessedEvent toProcessedEvent(TicketEvent event) {
        TicketProcessedEvent processedEvent = new TicketProcessedEvent();
        processedEvent.setTicketId(event.getTicketId());
        processedEvent.setPlayerId(event.getPlayerId());
        processedEvent.setGameId(event.getGameId());
        processedEvent.setRoundId(event.getRoundId());
        processedEvent.setBetAmount(event.getBetAmount());
        processedEvent.setWinAmount(event.getWinAmount());
        processedEvent.setCurrency(event.getCurrency());
        processedEvent.setStatus(event.getStatus());
        processedEvent.setEventTime(event.getEventTime());
        return processedEvent;
    }

    private static String toProcessedJson(TicketEvent event) {
        return JsonUtils.toJson(toProcessedEvent(event));
    }

    public static class UpdatePlayerTotalBetRedisAsyncFunction
            extends RichAsyncFunction<TicketEvent, TicketEvent> {

        private final String redisHost;
        private final int redisPort;
        private transient Counter redisSuccessCounter;
        private transient Counter redisFailureCounter;

        private transient RedisAsyncCommands<String, String> asyncCommands;
        private transient StatefulRedisConnection<String, String> connection;
        private transient RedisClient redisClient;

        public UpdatePlayerTotalBetRedisAsyncFunction(String redisHost, int redisPort) {
            this.redisHost = redisHost;
            this.redisPort = redisPort;
        }

        @Override
        public void open(Configuration parameters) {
            redisClient = RedisClient.create("redis://" + redisHost + ":" + redisPort);
            connection = redisClient.connect();
            asyncCommands = connection.async();

            MetricGroup metricGroup = getRuntimeContext().getMetricGroup();
            redisSuccessCounter = metricGroup.counter("ticket_redis_update_success_total");
            redisFailureCounter = metricGroup.counter("ticket_redis_update_failure_total");
        }

        @Override
        public void asyncInvoke(TicketEvent event, ResultFuture<TicketEvent> resultFuture) {
            String key = "player:total_bet:" + event.getPlayerId();
            double increment = event.getBetAmount().doubleValue();

            asyncCommands.incrbyfloat(key, increment)
                    .whenComplete((res, ex) -> {
                        if (ex != null) {
                            redisFailureCounter.inc();
                            resultFuture.complete(Collections.singleton(event));
                        } else {
                            redisSuccessCounter.inc();
                            resultFuture.complete(Collections.singleton(event));
                        }
                    });
        }

        @Override
        public void timeout(TicketEvent input, ResultFuture<TicketEvent> resultFuture) {
            redisFailureCounter.inc();
            resultFuture.complete(Collections.singleton(input));
        }

        @Override
        public void close() {
            if (connection != null) {
                connection.close();
            }
            if (redisClient != null) {
                redisClient.shutdown();
            }
        }
    }

    public static class WagerProcessMapFunction extends RichMapFunction<TicketEvent, TicketEvent> {

        private final String clickHouseUrl;
        private final String clickHouseUsername;
        private final String clickHousePassword;
        private final String redisHost;
        private final int redisPort;

        private transient WagerProcessingService wagerProcessingService;
        private transient LettuceConnectionFactory redisConnectionFactory;
        private transient StringRedisTemplate redisTemplate;

        public WagerProcessMapFunction(String clickHouseUrl,
            String clickHouseUsername,
            String clickHousePassword,
            String redisHost,
            int redisPort) {
            this.clickHouseUrl = clickHouseUrl;
            this.clickHouseUsername = clickHouseUsername;
            this.clickHousePassword = clickHousePassword;
            this.redisHost = redisHost;
            this.redisPort = redisPort;
        }

        @Override
        public void open(Configuration parameters) {
            WagerClickHouseGateway clickHouseGateway = new JdbcWagerClickHouseGateway(
                clickHouseUrl,
                clickHouseUsername,
                clickHousePassword
            );

            redisConnectionFactory = new LettuceConnectionFactory(redisHost, redisPort);
            redisConnectionFactory.afterPropertiesSet();

            redisTemplate = new StringRedisTemplate(redisConnectionFactory);
            redisTemplate.afterPropertiesSet();

            WagerIdempotencyRepository idempotencyRepository =
                new RedisWagerIdempotencyRepository(
                    redisTemplate,
                    "wager:idempotency",
                    Duration.ofDays(1)
                );

            wagerProcessingService = new WagerProcessingService(
                clickHouseGateway,
                idempotencyRepository,
                new WagerIdempotencyKeyResolver()
            );
        }

        @Override
        public TicketEvent map(TicketEvent event) {
            WagerProcessResult result = wagerProcessingService.process(event);

            if (result == WagerProcessResult.WRITTEN) {
                return event;
            }

            if (result == WagerProcessResult.DUPLICATE_SKIPPED) {
                return event;
            }

            throw new WagerProcessException("Unexpected wager process result");
        }

        @Override
        public void close() throws Exception {
            if (redisConnectionFactory != null) {
                redisConnectionFactory.destroy();
            }
            super.close();
        }
    }
}