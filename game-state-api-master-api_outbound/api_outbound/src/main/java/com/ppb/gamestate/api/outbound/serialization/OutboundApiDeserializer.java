package com.ppb.gamestate.api.outbound.serialization;

import com.ppb.gamestate.api.outbound.GameStateOutbound.Api;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

@Slf4j
public class OutboundApiDeserializer implements Deserializer<Api> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public Api deserialize(String topic, byte[] bytes) {
        return deserialize(bytes);
    }

    public Api deserialize(byte[] bytes) {
        Api api = null;
        try {
            api = Api.parseFrom(bytes);
        } catch (Exception exception) {
            log.error("Error in deserializing bytes to outbound Api", exception);
        }
        return api;
    }

    @Override
    public void close() {
    }
}
