package com.example.flinkpipelinedemo.modules.player.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class PlayerEvent {

    private String playerId;
    private String action;
    private String gameId;
}