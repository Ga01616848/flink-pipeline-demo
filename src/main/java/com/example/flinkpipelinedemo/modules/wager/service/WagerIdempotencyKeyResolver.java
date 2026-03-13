package com.example.flinkpipelinedemo.modules.wager.service;

import com.example.flinkpipelinedemo.modules.wager.model.TicketEvent;

public class WagerIdempotencyKeyResolver {

    public String resolve(TicketEvent event) {
        return safe(event.getTicketId())
                + "|" + safe(event.getStatus())
                + "|" + safe(event.getUpdatedAt())
                + "|" + safe(event.getEventId());
    }

    private String safe(Object value) {
        return value == null ? "" : String.valueOf(value).trim();
    }
}