package com.example.common;

import com.example.flinkpipelinedemo.modules.player.model.PlayerEvent;
import com.example.flinkpipelinedemo.shared.util.JsonUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JsonUtilsTest {

    @Test
    void testParsePlayerEvent() {
        String json = "{\"playerId\":\"p100\",\"action\":\"login\",\"gameId\":\"g01\"}";

        PlayerEvent event = JsonUtils.parse(json, PlayerEvent.class);

        assertNotNull(event);
        assertEquals("p100", event.getPlayerId());
        assertEquals("login", event.getAction());
        assertEquals("g01", event.getGameId());
    }
}