package com.example.flinkpipelinedemo.modules.wager.sink;

import com.example.flinkpipelinedemo.modules.wager.model.TicketEvent;
import com.example.flinkpipelinedemo.modules.wager.service.WagerIdempotencyKeyResolver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class JdbcWagerClickHouseGateway implements WagerClickHouseGateway {

    private static final String INSERT_SQL = """
            INSERT INTO default.wager_ticket_events
            (
                idempotency_key,
                ticket_id,
                event_id,
                player_id,
                game_id,
                round_id,
                bet_amount,
                valid_bet_amount,
                win_amount,
                payout_amount,
                currency,
                status,
                platform,
                device_type,
                event_time,
                created_at,
                updated_at
            )
            VALUES
            (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """;

    private final String url;
    private final String username;
    private final String password;
    private final WagerIdempotencyKeyResolver keyResolver = new WagerIdempotencyKeyResolver();

    public JdbcWagerClickHouseGateway(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    @Override
    public void upsert(TicketEvent event) throws Exception {
        String idempotencyKey = keyResolver.resolve(event);

        try (Connection connection = DriverManager.getConnection(url, username, password);
            PreparedStatement statement = connection.prepareStatement(INSERT_SQL)) {

            statement.setString(1, idempotencyKey);
            statement.setString(2, event.getTicketId());
            statement.setString(3, event.getEventId());
            statement.setString(4, event.getPlayerId());
            statement.setString(5, event.getGameId());
            statement.setString(6, event.getRoundId());
            statement.setBigDecimal(7, event.getBetAmount());
            statement.setBigDecimal(8, event.getValidBetAmount());
            statement.setBigDecimal(9, event.getWinAmount());
            statement.setBigDecimal(10, event.getPayoutAmount());
            statement.setString(11, event.getCurrency());
            statement.setString(12, event.getStatus());
            statement.setString(13, event.getPlatform());
            statement.setString(14, event.getDeviceType());
            statement.setLong(15, safeLong(event.getEventTime()));
            statement.setLong(16, safeLong(event.getCreatedAt()));
            statement.setLong(17, safeLong(event.getUpdatedAt()));

            statement.executeUpdate();
        }
    }

    private long safeLong(Long value) {
        return value == null ? 0L : value;
    }
}