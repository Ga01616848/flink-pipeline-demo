package com.example.flinkpipelinedemo.infrastructure.clickhouse.sink;

import org.apache.flink.connector.jdbc.JdbcExecutionOptions;

public class ClickHouseSinkConfig {

    public static JdbcExecutionOptions getExecutionOptions() {
        return JdbcExecutionOptions.builder()
                .withBatchSize(20000)
                .withBatchIntervalMs(200)
                .withMaxRetries(1)
                .build();
    }

    private ClickHouseSinkConfig() {
    }
}