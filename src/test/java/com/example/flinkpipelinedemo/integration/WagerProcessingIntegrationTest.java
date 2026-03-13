package com.example.flinkpipelinedemo.integration;

import com.example.flinkpipelinedemo.modules.wager.dedup.RedisWagerIdempotencyRepository;
import com.example.flinkpipelinedemo.modules.wager.dedup.WagerIdempotencyRepository;
import com.example.flinkpipelinedemo.modules.wager.model.TicketEvent;
import com.example.flinkpipelinedemo.modules.wager.service.WagerIdempotencyKeyResolver;
import com.example.flinkpipelinedemo.modules.wager.service.WagerProcessResult;
import com.example.flinkpipelinedemo.modules.wager.service.WagerProcessingService;
import com.example.flinkpipelinedemo.modules.wager.sink.JdbcWagerClickHouseGateway;
import com.example.flinkpipelinedemo.modules.wager.sink.WagerClickHouseGateway;
import com.example.flinkpipelinedemo.shared.config.AppProperties;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.example.flinkpipelinedemo.modules.wager.exception.WagerProcessException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = {
    "spring.main.web-application-type=none",
    "app.flink.job.enabled=false"
})
class WagerProcessingIntegrationTest {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private AppProperties appProperties;

    private WagerProcessingService service;
    private WagerIdempotencyKeyResolver keyResolver;

    @BeforeEach
    void setUp() throws Exception {
        keyResolver = new WagerIdempotencyKeyResolver();

        WagerIdempotencyRepository repository =
            new RedisWagerIdempotencyRepository(
                redisTemplate,
                "wager:idempotency",
                Duration.ofDays(1)
            );

        WagerClickHouseGateway gateway =
            new JdbcWagerClickHouseGateway(
                appProperties.getClickhouse().getUrl(),
                appProperties.getClickhouse().getUsername(),
                appProperties.getClickhouse().getPassword()
            );

        service = new WagerProcessingService(
            gateway,
            repository,
            keyResolver
        );

        cleanupTestData();
    }

    /**
     * 驗證當一筆新的 TicketEvent 被處理時，
     * 會真的寫入 ClickHouse，並且真的在 Redis 寫入 idempotency key。
     */
    @Test
    void shouldWriteToClickHouseAndRedisWhenEventIsFirstSeen() throws Exception {
        TicketEvent event = buildEvent();
        String idempotencyKey = keyResolver.resolve(event);
        String redisKey = "wager:idempotency:" + idempotencyKey;

        System.out.println("=== INTEGRATION TEST START ===");
        System.out.println("eventId = " + event.getEventId());
        System.out.println("idempotencyKey = " + idempotencyKey);

        WagerProcessResult result = service.process(event);

        System.out.println("result = " + result);

        Boolean redisExists = redisTemplate.hasKey(redisKey);
        System.out.println("redis key exists = " + redisExists);

        int clickHouseCount = queryClickHouseCountByEventId(event.getEventId());
        System.out.println("clickhouse row count = " + clickHouseCount);

        assertEquals(WagerProcessResult.WRITTEN, result);
        assertTrue(Boolean.TRUE.equals(redisExists));
        assertEquals(1, clickHouseCount);

        System.out.println("=== INTEGRATION TEST END ===");
    }

    /**
     * 整合測試：
     * 驗證同一筆 TicketEvent 被重複處理時，
     * 第一次會正常寫入 ClickHouse 與 Redis，
     * 第二次會因為 Redis 已存在相同 idempotency key 而被判定為 duplicate，
     * 因此不會再次寫入 ClickHouse。
     *
     * 驗收重點：
     * 1. 第一次處理結果為 WRITTEN
     * 2. 第二次處理結果為 DUPLICATE_SKIPPED
     * 3. Redis idempotency key 仍存在
     * 4. ClickHouse 中相同 event_id 只會有 1 筆資料
     */
    @Test
    void shouldSkipDuplicateEventAndNotWriteToClickHouseTwice() throws Exception {
        TicketEvent event = buildEvent();
        String idempotencyKey = keyResolver.resolve(event);
        String redisKey = "wager:idempotency:" + idempotencyKey;

        System.out.println("=== DUPLICATE INTEGRATION TEST START ===");
        System.out.println("eventId = " + event.getEventId());
        System.out.println("idempotencyKey = " + idempotencyKey);

        WagerProcessResult firstResult = service.process(event);
        WagerProcessResult secondResult = service.process(event);

        System.out.println("firstResult = " + firstResult);
        System.out.println("secondResult = " + secondResult);

        Boolean redisExists = redisTemplate.hasKey(redisKey);
        int clickHouseCount = queryClickHouseCountByEventId(event.getEventId());

        System.out.println("redis key exists = " + redisExists);
        System.out.println("clickhouse row count = " + clickHouseCount);
        System.out.println("=== DUPLICATE INTEGRATION TEST END ===");

        assertEquals(WagerProcessResult.WRITTEN, firstResult);
        assertEquals(WagerProcessResult.DUPLICATE_SKIPPED, secondResult);
        assertTrue(Boolean.TRUE.equals(redisExists));
        assertEquals(1, clickHouseCount);
    }

    /**
     * ClickHouse 寫入失敗時 service拋 WagerProcessException Redis 不會有 idempotency key
     * ClickHouse 也不會有成功寫入的資料
     * 驗收重點：
     * 1. process(event) 會拋出 WagerProcessException
     * 2. Redis 不存在該筆事件的 idempotency key
     * 3. ClickHouse 中該 event_id 筆數仍為 0
     *
     * ClickHouse 暫時異常，事件不會被誤標記為已處理
     */
    @Test
    void shouldNotMarkRedisWhenClickHouseWriteFails() throws Exception {
        TicketEvent event = buildEvent();

        String idempotencyKey = keyResolver.resolve(event);
        String redisKey = "wager:idempotency:" + idempotencyKey;

        System.out.println("=== FAILURE INTEGRATION TEST START ===");
        System.out.println("eventId = " + event.getEventId());
        System.out.println("idempotencyKey = " + idempotencyKey);

        WagerClickHouseGateway failingGateway =
            new JdbcWagerClickHouseGateway(
                "jdbc:clickhouse://localhost:8123/default?socket_timeout=1000",
                "wrong-user",
                "wrong-password"
            );

        WagerIdempotencyRepository repository =
            new RedisWagerIdempotencyRepository(
                redisTemplate,
                "wager:idempotency",
                Duration.ofDays(1)
            );

        WagerProcessingService failingService = new WagerProcessingService(
            failingGateway,
            repository,
            keyResolver
        );

        assertThrows(WagerProcessException.class, () -> {
            failingService.process(event);
        });

        Boolean redisExists = redisTemplate.hasKey(redisKey);
        int clickHouseCount = queryClickHouseCountByEventId(event.getEventId());

        System.out.println("redis key exists = " + redisExists);
        System.out.println("clickhouse row count = " + clickHouseCount);
        System.out.println("=== FAILURE INTEGRATION TEST END ===");

        assertTrue(redisExists == null || !redisExists);
        assertEquals(0, clickHouseCount);
    }

    private void cleanupTestData() throws Exception {
        String eventId = "event-it-1001";
        String idempotencyKey = "ticket-it-1001|SETTLED|1710000001000|event-it-1001";
        String redisKey = "wager:idempotency:" + idempotencyKey;

        redisTemplate.delete(redisKey);

        try (Connection connection = DriverManager.getConnection(
            appProperties.getClickhouse().getUrl(),
            appProperties.getClickhouse().getUsername(),
            appProperties.getClickhouse().getPassword());
            PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM default.wager_ticket_events WHERE event_id = ?")) {

            statement.setString(1, eventId);
            statement.executeUpdate();
        } catch (Exception ex) {
            System.out.println("cleanup clickhouse failed: " + ex.getMessage());
        }
    }

    private int queryClickHouseCountByEventId(String eventId) throws Exception {
        try (Connection connection = DriverManager.getConnection(
            appProperties.getClickhouse().getUrl(),
            appProperties.getClickhouse().getUsername(),
            appProperties.getClickhouse().getPassword());
            PreparedStatement statement = connection.prepareStatement(
                "SELECT count(*) FROM default.wager_ticket_events WHERE event_id = ?")) {

            statement.setString(1, eventId);

            try (ResultSet rs = statement.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    private TicketEvent buildEvent() {
        TicketEvent event = new TicketEvent();
        event.setTicketId("ticket-it-1001");
        event.setEventId("event-it-1001");
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