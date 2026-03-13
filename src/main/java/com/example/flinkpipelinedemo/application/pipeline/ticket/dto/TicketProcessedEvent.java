package com.example.flinkpipelinedemo.application.pipeline.ticket.dto;

import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

@Getter
@Setter
public class TicketProcessedEvent {
    private String ticketId;
    private String playerId;
    private String gameId;
    private String roundId;
    private BigDecimal betAmount;
    private BigDecimal winAmount;
    private String currency;
    private String status;
    private Long eventTime;
}