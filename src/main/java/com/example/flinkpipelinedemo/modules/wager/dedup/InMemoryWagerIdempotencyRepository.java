package com.example.flinkpipelinedemo.modules.wager.dedup;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryWagerIdempotencyRepository implements WagerIdempotencyRepository {

    private final Set<String> processedKeys = ConcurrentHashMap.newKeySet();

    @Override
    public boolean isProcessed(String idempotencyKey) {
        return processedKeys.contains(idempotencyKey);
    }

    @Override
    public void markProcessed(String idempotencyKey) {
        processedKeys.add(idempotencyKey);
    }

    public int size() {
        return processedKeys.size();
    }
}