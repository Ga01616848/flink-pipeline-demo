package com.example.flinkpipelinedemo.modules.wager.service;

import com.example.flinkpipelinedemo.modules.wager.dedup.WagerIdempotencyRepository;
import com.example.flinkpipelinedemo.modules.wager.exception.WagerProcessException;
import com.example.flinkpipelinedemo.modules.wager.model.TicketEvent;
import com.example.flinkpipelinedemo.modules.wager.sink.WagerClickHouseGateway;

public class WagerProcessingService {

    private final WagerClickHouseGateway clickHouseGateway;
    private final WagerIdempotencyRepository idempotencyRepository;
    private final WagerIdempotencyKeyResolver keyResolver;

    public WagerProcessingService(WagerClickHouseGateway clickHouseGateway,
                                  WagerIdempotencyRepository idempotencyRepository,
                                  WagerIdempotencyKeyResolver keyResolver) {
        this.clickHouseGateway = clickHouseGateway;
        this.idempotencyRepository = idempotencyRepository;
        this.keyResolver = keyResolver;
    }

    public WagerProcessResult process(TicketEvent event) {
        validate(event);

        String idempotencyKey = keyResolver.resolve(event);
        if (idempotencyRepository.isProcessed(idempotencyKey)) {
            return WagerProcessResult.DUPLICATE_SKIPPED;
        }

        try {
            clickHouseGateway.upsert(event);
            idempotencyRepository.markProcessed(idempotencyKey);
            return WagerProcessResult.WRITTEN;
        } catch (Exception ex) {
            throw new WagerProcessException(
                    "ClickHouse write failed, keep Kafka offset uncommitted for reconsume",
                    ex
            );
        }
    }

    private void validate(TicketEvent event) {
        if (event == null) {
            throw new WagerProcessException("ticket event is null");
        }
        if (isBlank(event.getTicketId())) {
            throw new WagerProcessException("ticketId is blank");
        }
        if (isBlank(event.getPlayerId())) {
            throw new WagerProcessException("playerId is blank");
        }
        if (isBlank(event.getStatus())) {
            throw new WagerProcessException("status is blank");
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}