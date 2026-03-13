package com.example.flinkpipelinedemo.modules.wager.enums;

public enum TicketStatus {
    PLACED,
    SETTLED,
    CANCELLED;

    public static boolean isValid(String value) {
        if (value == null || value.isBlank()) {
            return false;
        }

        for (TicketStatus status : TicketStatus.values()) {
            if (status.name().equalsIgnoreCase(value.trim())) {
                return true;
            }
        }

        return false;
    }
}