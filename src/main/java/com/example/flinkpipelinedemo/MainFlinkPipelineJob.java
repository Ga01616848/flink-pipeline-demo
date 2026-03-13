package com.example.flinkpipelinedemo;

import com.example.flinkpipelinedemo.infrastructure.retry.job.RetryPipeline;
import com.example.flinkpipelinedemo.modules.player.job.PlayerPipeline;
import com.example.flinkpipelinedemo.modules.wager.job.TicketPipeline;
import com.example.flinkpipelinedemo.shared.config.AppProperties;
import com.example.flinkpipelinedemo.shared.config.FlinkEnvConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class MainFlinkPipelineJob implements CommandLineRunner {

    private final AppProperties appProperties;
    private final PlayerPipeline playerPipeline;
    private final TicketPipeline ticketPipeline;
    private final RetryPipeline retryPipeline;

    public MainFlinkPipelineJob(AppProperties appProperties,
                                PlayerPipeline playerPipeline,
                                TicketPipeline ticketPipeline,
                                RetryPipeline retryPipeline) {
        this.appProperties = appProperties;
        this.playerPipeline = playerPipeline;
        this.ticketPipeline = ticketPipeline;
        this.retryPipeline = retryPipeline;
    }

    @Override
    public void run(String... args) throws Exception {

        System.out.println("MainFlinkPipeline started");

        StreamExecutionEnvironment env = FlinkEnvConfig.createEnv(
                appProperties.getFlink().getDefaultParallelism(),
                appProperties.getFlink().getCheckpointIntervalMs(),
                appProperties.getFlink().getCheckpointStorage(),
                appProperties.getFlink().getMinPauseBetweenCheckpointsMs(),
                appProperties.getFlink().getCheckpointTimeoutMs()
        );

        playerPipeline.build(env);
        ticketPipeline.build(env);
        retryPipeline.build(env);


        env.execute("Flink-Pipeline-Main-Job");
    }
}