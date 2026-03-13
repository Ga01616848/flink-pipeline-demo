package com.example.flinkpipelinedemo.modules.wager.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@NoArgsConstructor
public class TicketEvent {

    private String ticketId;
    private String eventId;
    private String playerId;
    private String gameId;
    private String roundId;

    private BigDecimal betAmount;
    private BigDecimal validBetAmount;
    private BigDecimal winAmount;
    private BigDecimal payoutAmount;

    private String currency;
    private String status;
    private String platform;
    private String deviceType;

    private Long eventTime;
    private Long createdAt;
    private Long updatedAt;
}