package com.example.flinkpipelinedemo.infrastructure.redis;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
public class PlayerBetRedisService {

    private final StringRedisTemplate stringRedisTemplate;

    public PlayerBetRedisService(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void addToTotalBet(String playerId, BigDecimal betAmount) {
        if (playerId == null || playerId.isBlank() || betAmount == null) {
            return;
        }

        String key = buildTotalBetKey(playerId);
        String currentValue = stringRedisTemplate.opsForValue().get(key);

        BigDecimal currentTotal = currentValue == null
                ? BigDecimal.ZERO
                : new BigDecimal(currentValue);

        BigDecimal newTotal = currentTotal.add(betAmount);
        stringRedisTemplate.opsForValue().set(key, newTotal.toPlainString());
    }

    public String getTotalBet(String playerId) {
        return stringRedisTemplate.opsForValue().get(buildTotalBetKey(playerId));
    }

    private String buildTotalBetKey(String playerId) {
        return "player:" + playerId + ":total_bet";
    }
}