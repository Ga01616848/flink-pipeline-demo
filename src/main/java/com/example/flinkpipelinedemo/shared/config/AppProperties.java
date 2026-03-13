package com.example.flinkpipelinedemo.shared.config;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "app")
@Getter
@Setter
public class AppProperties {

    private Kafka kafka = new Kafka();
    private Flink flink = new Flink();
    private Retry retry = new Retry();
    private Clickhouse clickhouse;
    private Redis redis;

    @Data
    public static class Kafka {
        private String bootstrapServers;
        private String retryTopic;
        private String deadLetterTopic;
        private String playerTopic;
        private String ticketTopic;
        private String ticketProcessedTopic;   // 新增
        private String playerGroupId;
        private String ticketGroupId;
        private String retryGroupId;
    }

    @Data
    public static class Flink {
        private int defaultParallelism;
        private long checkpointIntervalMs;
        private long minPauseBetweenCheckpointsMs;
        private long checkpointTimeoutMs;
        private String checkpointStorage;
    }

    @Data
    public static class Retry {
        private int maxRetryCount;
        private long retryBackoffMs;
    }

    @Data
    public static class Clickhouse {
        private String url;
        private String username;
        private String password;
    }
    @Data
    public static class Redis {

        private String host;
        private int port;
    }
}
