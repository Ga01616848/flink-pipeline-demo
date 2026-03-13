package com.example.flinkpipelinedemo.modules.wager.dedup;

public interface WagerIdempotencyRepository {

    boolean isProcessed(String idempotencyKey);

    void markProcessed(String idempotencyKey);
}