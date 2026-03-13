package com.example.retry;

import com.example.flinkpipelinedemo.infrastructure.retry.model.RetryEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RetryEventTest {

    @Test
    void testRetryEventCreation() {
        RetryEvent event = new RetryEvent();
        event.setSourceTopic("ticket-topic-v2");
        event.setRawData("bad-data");
        event.setErrorMessage("JSON parse error");
        event.setRetryCount(1);

        assertEquals("ticket-topic-v2", event.getSourceTopic());
        assertEquals("bad-data", event.getRawData());
        assertEquals("JSON parse error", event.getErrorMessage());
        assertEquals(1, event.getRetryCount());
    }

    @Test
    void testRetryCountIncrease() {
        RetryEvent event = new RetryEvent();
        event.setSourceTopic("ticket-topic-v2");
        event.setRawData("bad-data");
        event.setErrorMessage("JSON parse error");
        event.setRetryCount(1);

        event.setRetryCount(event.getRetryCount() + 1);

        assertEquals(2, event.getRetryCount());
    }
}