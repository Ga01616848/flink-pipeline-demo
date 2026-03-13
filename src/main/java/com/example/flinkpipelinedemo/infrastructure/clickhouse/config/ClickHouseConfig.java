package com.example.flinkpipelinedemo.infrastructure.clickhouse.config;

import com.example.flinkpipelinedemo.shared.config.AppProperties;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;

public class ClickHouseConfig {

    public static JdbcConnectionOptions getOptions(AppProperties props) {

        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(props.getClickhouse().getUrl())
                .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                .withUsername(props.getClickhouse().getUsername())
                .withPassword(props.getClickhouse().getPassword())
                .build();
    }
}