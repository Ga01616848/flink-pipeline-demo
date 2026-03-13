package com.example.flinkpipelinedemo.modules.wager.dedup;

import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;

public class RedisWagerIdempotencyRepository implements WagerIdempotencyRepository {

    private final StringRedisTemplate redisTemplate;
    private final String keyPrefix;
    private final Duration ttl;

    public RedisWagerIdempotencyRepository(StringRedisTemplate redisTemplate,
        String keyPrefix,
        Duration ttl) {
        this.redisTemplate = redisTemplate;
        this.keyPrefix = keyPrefix;
        this.ttl = ttl;
    }

    @Override
    public boolean tryMarkProcessed(String idempotencyKey) {
        Boolean success = redisTemplate.opsForValue().setIfAbsent(
            buildKey(idempotencyKey),
            "1",
            ttl
        );
        return Boolean.TRUE.equals(success);
    }

    @Override
    public void unmarkProcessed(String idempotencyKey) {
        redisTemplate.delete(buildKey(idempotencyKey));
    }

    private String buildKey(String idempotencyKey) {
        return keyPrefix + ":" + idempotencyKey;
    }
}