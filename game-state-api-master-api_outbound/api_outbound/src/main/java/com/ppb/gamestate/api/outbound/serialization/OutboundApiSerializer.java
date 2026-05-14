package com.ppb.gamestate.api.outbound.serialization;

import com.ppb.gamestate.api.outbound.GameStateOutbound.Api;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class OutboundApiSerializer implements Serializer<Api> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public byte[] serialize(String topic, Api api) {
        return serialize(api);
    }

    public byte[] serialize(Api api) {
        return api.toByteArray();
    }

    @Override
    public void close() {
    }
}
