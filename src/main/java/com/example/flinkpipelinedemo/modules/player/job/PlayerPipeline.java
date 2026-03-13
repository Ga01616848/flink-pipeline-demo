package com.example.flinkpipelinedemo.modules.player.job;

import com.example.flinkpipelinedemo.infrastructure.clickhouse.config.ClickHouseConfig;
import com.example.flinkpipelinedemo.infrastructure.clickhouse.sink.ClickHouseSinkConfig;
import com.example.flinkpipelinedemo.infrastructure.retry.model.RetryEvent;
import com.example.flinkpipelinedemo.infrastructure.retry.producer.RetryProducer;
import com.example.flinkpipelinedemo.modules.player.model.PlayerEvent;
import com.example.flinkpipelinedemo.shared.config.AppProperties;
import com.example.flinkpipelinedemo.shared.util.JsonUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class PlayerPipeline {

    private final AppProperties props;

    public PlayerPipeline(AppProperties props) {
        this.props = props;
    }

    public void build(StreamExecutionEnvironment env) {
        String bootstrapServers = props.getKafka().getBootstrapServers();
        String playerTopic = props.getKafka().getPlayerTopic();
        String playerGroupId = props.getKafka().getPlayerGroupId();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(playerTopic)
                .setGroupId(playerGroupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Player Kafka Source");

        DataStream<PlayerEvent> playerStream = kafkaStream
                .map(new PlayerEventParserAndValidator(playerTopic))
                .filter(event -> event != null);

        DataStream<PlayerEvent> enrichedPlayerStream = playerStream
                .map(new UpdatePlayerOnlineOfflineCountRedisFunction(
                        props.getRedis().getHost(),
                        props.getRedis().getPort()
                ))
                .name("update-player-online-offline-count-redis");

        enrichedPlayerStream
                .map(new PlayerEventToStringMapper())
                .print("Player event");

        enrichedPlayerStream.addSink(
                org.apache.flink.connector.jdbc.JdbcSink.sink(
                        "INSERT INTO player_data (player_id, action, game_id) VALUES (?, ?, ?)",
                        (statement, event) -> {
                            statement.setString(1, event.getPlayerId());
                            statement.setString(2, event.getAction());
                            statement.setString(3, event.getGameId());
                        },
                        ClickHouseSinkConfig.getExecutionOptions(),
                        ClickHouseConfig.getOptions(props)
                )
        );
    }

    public static class PlayerEventParserAndValidator implements MapFunction<String, PlayerEvent> {
        private final String sourceTopic;

        public PlayerEventParserAndValidator(String sourceTopic) {
            this.sourceTopic = sourceTopic;
        }

        @Override
        public PlayerEvent map(String value) {
            System.out.println("Player raw = " + value);

            String actualRawData = value;
            int retryCount = 0;

            try {
                RetryEvent retryEvent = tryParseRetryEvent(value);
                if (retryEvent != null) {
                    System.out.println("PlayerPipeline detected RetryEvent, retryCount = " + retryEvent.getRetryCount());

                    actualRawData = retryEvent.getRawData();
                    retryCount = retryEvent.getRetryCount() == null ? 0 : retryEvent.getRetryCount();
                }

                PlayerEvent event = JsonUtils.parse(actualRawData, PlayerEvent.class);

                String validationError = validate(event);
                if (validationError != null) {
                    System.out.println("Player validate failed: " + validationError + ", raw = " + actualRawData);
                    sendToRetry(actualRawData, validationError, retryCount);
                    return null;
                }

                return event;

            } catch (Exception e) {
                System.out.println("Player parse failed, send to retry-topic, raw = " + actualRawData + ", error=" + e.getMessage());
                sendToRetry(actualRawData, e.getMessage(), retryCount);
                return null;
            }
        }

        private RetryEvent tryParseRetryEvent(String value) {
            try {
                RetryEvent retryEvent = JsonUtils.parse(value, RetryEvent.class);

                if (retryEvent == null) {
                    return null;
                }

                if (retryEvent.getRawData() == null || retryEvent.getRawData().isBlank()) {
                    return null;
                }

                if (retryEvent.getSourceTopic() == null || retryEvent.getSourceTopic().isBlank()) {
                    return null;
                }

                return retryEvent;
            } catch (Exception e) {
                return null;
            }
        }

        private void sendToRetry(String rawData, String errorMessage, int retryCount) {
            RetryEvent retryEvent = new RetryEvent();
            retryEvent.setSourceTopic(sourceTopic);
            retryEvent.setRawData(rawData);
            retryEvent.setErrorMessage(errorMessage);
            retryEvent.setRetryCount(retryCount + 1);
            RetryProducer.sendToRetryTopic(retryEvent);
        }

        private String validate(PlayerEvent event) {
            if (event == null) {
                return "event is null";
            }

            if (isBlank(event.getPlayerId())) {
                return "playerId is blank";
            }

            if (isBlank(event.getAction())) {
                return "action is blank";
            }

            if (isBlank(event.getGameId())) {
                return "gameId is blank";
            }

            if (!isValidAction(event.getAction())) {
                return "invalid action";
            }

            return null;
        }

        private boolean isValidAction(String action) {
            if (action == null || action.isBlank()) {
                return false;
            }

            String normalized = action.trim().toUpperCase();
            return normalized.equals("ONLINE") || normalized.equals("OFFLINE");
        }

        private boolean isBlank(String value) {
            return value == null || value.trim().isEmpty();
        }
    }

    public static class UpdatePlayerOnlineOfflineCountRedisFunction extends RichMapFunction<PlayerEvent, PlayerEvent> {

        private final String redisHost;
        private final int redisPort;

        private transient LettuceConnectionFactory redisConnectionFactory;
        private transient StringRedisTemplate stringRedisTemplate;

        public UpdatePlayerOnlineOfflineCountRedisFunction(String redisHost, int redisPort) {
            this.redisHost = redisHost;
            this.redisPort = redisPort;
        }

        @Override
        public void open(Configuration parameters) {
            RedisStandaloneConfiguration redisConfig =
                    new RedisStandaloneConfiguration(redisHost, redisPort);

            this.redisConnectionFactory = new LettuceConnectionFactory(redisConfig);
            this.redisConnectionFactory.afterPropertiesSet();

            this.stringRedisTemplate = new StringRedisTemplate();
            this.stringRedisTemplate.setConnectionFactory(redisConnectionFactory);
            this.stringRedisTemplate.afterPropertiesSet();
        }

        @Override
        public PlayerEvent map(PlayerEvent event) {
            if (event == null || event.getPlayerId() == null || event.getPlayerId().isBlank()) {
                return event;
            }

            String action = event.getAction() == null ? "" : event.getAction().trim().toUpperCase();

            if ("ONLINE".equals(action)) {
                increment("player:" + event.getPlayerId() + ":online_count");
            } else if ("OFFLINE".equals(action)) {
                increment("player:" + event.getPlayerId() + ":offline_count");
            }

            return event;
        }

        private void increment(String key) {
            String currentValue = stringRedisTemplate.opsForValue().get(key);
            long currentCount = currentValue == null ? 0L : Long.parseLong(currentValue);
            long newCount = currentCount + 1;
            stringRedisTemplate.opsForValue().set(key, String.valueOf(newCount));
        }

        @Override
        public void close() {
            if (redisConnectionFactory != null) {
                redisConnectionFactory.destroy();
            }
        }
    }

    public static class PlayerEventToStringMapper implements MapFunction<PlayerEvent, String> {
        @Override
        public String map(PlayerEvent event) {
            return "playerId=" + event.getPlayerId()
                    + ", action=" + event.getAction()
                    + ", gameId=" + event.getGameId();
        }
    }
}