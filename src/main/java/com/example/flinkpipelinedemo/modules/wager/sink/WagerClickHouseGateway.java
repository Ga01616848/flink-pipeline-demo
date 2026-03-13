package com.example.flinkpipelinedemo.modules.wager.sink;

import com.example.flinkpipelinedemo.modules.wager.model.TicketEvent;

public interface WagerClickHouseGateway {

    void upsert(TicketEvent event) throws Exception;
}