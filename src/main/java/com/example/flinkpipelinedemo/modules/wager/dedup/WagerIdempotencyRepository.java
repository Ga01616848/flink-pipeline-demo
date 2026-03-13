package com.example.flinkpipelinedemo.modules.wager.dedup;

public interface WagerIdempotencyRepository {

    boolean tryMarkProcessed(String idempotencyKey);

    void unmarkProcessed(String idempotencyKey);
}