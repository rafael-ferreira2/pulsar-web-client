package com.ppb.gamestate.api.outbound.serialization;

import com.ppb.gamestate.api.outbound.GameStateOutbound.Api;
import com.ppb.gamestate.api.outbound.util.OutboundApiFixture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OutboundApiSerializationTest {

    @Test
    @DisplayName("Outbound api serialization should serialize an outbound api object to byte array and deserialize the byte array back to the outbound api")
    public void testSerializingAnOutboundApi() {
        try(OutboundApiSerializer outboundApiSerializer = new OutboundApiSerializer();
            OutboundApiDeserializer outboundApiDeserializer = new OutboundApiDeserializer()) {
            final String GAME_TOPIC = "gamestateOutboundTopic";
            final Api api = OutboundApiFixture.BASE_STATE.build();
            final byte[] serializedOutboundApi = outboundApiSerializer.serialize(GAME_TOPIC, api);

            final Api deserializedApi = outboundApiDeserializer.deserialize(GAME_TOPIC, serializedOutboundApi);

            assertEquals(api, deserializedApi);
        }
    }

    @Test
    @DisplayName("Outbound api serialization should serialize an outbound api object without topic to byte array and deserialize the byte array back to the outbound api")
    public void testSerializingAnOutboundApiWithoutTopic() {
        try(OutboundApiSerializer outboundApiSerializer = new OutboundApiSerializer();
            final OutboundApiDeserializer outboundApiDeserializer = new OutboundApiDeserializer()){
            final Api api = OutboundApiFixture.BASE_STATE.build();
            final byte[] serializedOutboundApi = outboundApiSerializer.serialize(api);
            final Api deserializedApi = outboundApiDeserializer.deserialize(serializedOutboundApi);

            assertEquals(api, deserializedApi);
        }
    }
}
