package com.example.flinkpipelinedemo.modules.wager.dedup;

import java.util.HashSet;
import java.util.Set;

public class InMemoryWagerIdempotencyRepository implements WagerIdempotencyRepository {

    private final Set<String> processedKeys = new HashSet<>();

    @Override
    public boolean tryMarkProcessed(String idempotencyKey) {
        if (processedKeys.contains(idempotencyKey)) {
            return false;
        }
        processedKeys.add(idempotencyKey);
        return true;
    }

    @Override
    public void unmarkProcessed(String idempotencyKey) {
        processedKeys.remove(idempotencyKey);
    }

    public int size() {
        return processedKeys.size();
    }
}