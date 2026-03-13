package com.example.flinkpipelinedemo.shared.config;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkEnvConfig {

    private FlinkEnvConfig() {
    }

    public static StreamExecutionEnvironment createEnv(
            int parallelism,
            long checkpointIntervalMs,
            String checkpointStorage,
            long minPauseBetweenCheckpointsMs,
            long checkpointTimeoutMs
    ) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(parallelism);
        env.enableCheckpointing(checkpointIntervalMs);
        env.getCheckpointConfig().setCheckpointStorage(checkpointStorage);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseBetweenCheckpointsMs);
        env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeoutMs);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        return env;
    }
}