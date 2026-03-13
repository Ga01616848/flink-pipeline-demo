package com.example.flinkpipelinedemo.modules.wager.service;

import com.example.flinkpipelinedemo.modules.wager.dedup.InMemoryWagerIdempotencyRepository;
import com.example.flinkpipelinedemo.modules.wager.dedup.WagerIdempotencyRepository;
import com.example.flinkpipelinedemo.modules.wager.exception.WagerProcessException;
import com.example.flinkpipelinedemo.modules.wager.model.TicketEvent;
import com.example.flinkpipelinedemo.modules.wager.sink.WagerClickHouseGateway;

import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.*;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WagerProcessingServiceTest {

    /**
     * 測試情境 1：
     * ClickHouse 寫入失敗時，service 必須拋出 WagerProcessException，
     * 並且不能把 idempotency key 標記為 processed。
     *
     * 這可以確保：
     * - Flink operator 會因為 exception 而 faifaill
     * - job restart 後 Kafka 會 reconsume 這筆資料
     * - 不會因為提早 mark processed 而造成資料遺失
     */
    @Test
    void shouldThrowExceptionAndUnmarkProcessedWhenClickHouseWriteFails() throws Exception {
        WagerIdempotencyRepository repository = mock(WagerIdempotencyRepository.class);
        WagerClickHouseGateway gateway = mock(WagerClickHouseGateway.class);

        WagerProcessingService service = new WagerProcessingService(
            gateway,
            repository,
            new WagerIdempotencyKeyResolver()
        );

        TicketEvent event = buildEvent();
        String idempotencyKey = new WagerIdempotencyKeyResolver().resolve(event);

        System.out.println("=== TEST START ===");
        System.out.println("eventId = " + event.getEventId());
        System.out.println("idempotencyKey = " + idempotencyKey);

        when(repository.tryMarkProcessed(idempotencyKey)).thenReturn(true);
        doThrow(new RuntimeException("clickhouse down"))
            .when(gateway).upsert(event);

        assertThrows(WagerProcessException.class, () -> service.process(event));

        verify(repository, times(1)).tryMarkProcessed(idempotencyKey);
        verify(gateway, times(1)).upsert(event);
        verify(repository, times(1)).unmarkProcessed(idempotencyKey);

        System.out.println("=== TEST END ===");
    }

    /**
     * 測試：當一個「第一次出現的事件」被處理時，系統應該正確寫入 ClickHouse，
     * 並將該事件標記為已處理（idempotency mark）。
     *
     * 測試流程：
     * 1. 建立一個 InMemoryWagerIdempotencyRepository（模擬 idempotency storage）。
     * 2. 建立一個假的 ClickHouse Gateway，用來模擬 ClickHouse 寫入成功。
     * 3. 建立 WagerProcessingService 並注入 gateway、repository 與 key resolver。
     * 4. 建立一筆測試用的 TicketEvent。
     * 5. 呼叫 service.process(event)。
     *
     * 預期結果：
     * - service 應回傳 WagerProcessResult.WRITTEN
     * - ClickHouse gateway 應該被呼叫一次（writeCount = 1）
     * - repository 中應新增一筆 idempotency 紀錄（size = 1）
     *
     * 目的：
     * 驗證當事件是第一次被處理時：
     * - ClickHouse 會被寫入
     * - idempotency repository 會正確記錄該事件，避免未來重複處理
     */
    @Test
    void shouldWriteToClickHouseAndMarkProcessedWhenEventIsFirstSeen() {
        InMemoryWagerIdempotencyRepository repository = new InMemoryWagerIdempotencyRepository();
        AtomicInteger writeCount = new AtomicInteger(0);

        WagerClickHouseGateway gateway = event -> {
            System.out.println("[Gateway] ClickHouse write success, eventId=" + event.getEventId());
            writeCount.incrementAndGet();
        };

        WagerProcessingService service = new WagerProcessingService(
            gateway,
            repository,
            new WagerIdempotencyKeyResolver()
        );

        TicketEvent event = buildEvent();

        System.out.println("=== TEST START: first event should be written ===");
        System.out.println("Before process, repository size = " + repository.size());

        WagerProcessResult result = service.process(event);

        System.out.println("After process, result = " + result);
        System.out.println("After process, writeCount = " + writeCount.get());
        System.out.println("After process, repository size = " + repository.size());
        System.out.println("=== TEST END ===");

        assertEquals(WagerProcessResult.WRITTEN, result);
        assertEquals(1, writeCount.get());
        assertEquals(1, repository.size());
    }

    /**
     * 測試情境 2：
     * 同一筆 event 被重送（例如 Kafka ACK 網路波動導致 reconsume）。
     *
     * - 優先處理ClickHouse 正常寫入 idempotency key 被記錄
     * - 同 event 被識別為 duplicate
     * - 不應再寫 ClickHouse
     * - 回傳 DUPLICATE_SKIPPED
     *
     * 驗證重點：
     * ClickHouse 寫入次數只能是 1。
     */
    @Test
    void shouldSkipDuplicateWhenSameEventIsRedelivered() {
        InMemoryWagerIdempotencyRepository repository = new InMemoryWagerIdempotencyRepository();
        AtomicInteger writeCount = new AtomicInteger(0);

        WagerClickHouseGateway gateway = event -> writeCount.incrementAndGet();

        WagerProcessingService service = new WagerProcessingService(
            gateway,
            repository,
            new WagerIdempotencyKeyResolver()
        );

        TicketEvent event = buildEvent();

        WagerProcessResult firstResult = service.process(event);
        WagerProcessResult secondResult = service.process(event);

        assertEquals(WagerProcessResult.WRITTEN, firstResult);
        assertEquals(WagerProcessResult.DUPLICATE_SKIPPED, secondResult);
        assertEquals(1, writeCount.get());
        assertEquals(1, repository.size());
    }

   // 建立一個模擬的 TicketEvent 測試 WagerProcessingService。
        private TicketEvent buildEvent() {
        TicketEvent event = new TicketEvent();
        event.setTicketId("ticket-1001");
        event.setEventId("event-1001");
        event.setPlayerId("player-99");
        event.setGameId("game-1");
        event.setRoundId("round-1");
        event.setStatus("SETTLED");
        event.setCurrency("USD");
        event.setPlatform("WEB");
        event.setDeviceType("DESKTOP");
        event.setBetAmount(new BigDecimal("100.00"));
        event.setValidBetAmount(new BigDecimal("100.00"));
        event.setWinAmount(new BigDecimal("50.00"));
        event.setPayoutAmount(new BigDecimal("150.00"));
        event.setEventTime(1710000000000L);
        event.setCreatedAt(1710000000000L);
        event.setUpdatedAt(1710000001000L);
        return event;
    }
}