package com.ppb.gamestate.api.outbound.serialization;

import com.ppb.gamestate.api.outbound.GameStateOutbound;
import com.ppb.gamestate.api.outbound.util.TennisOutboundApiFixture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TennisOutboundApiSerializationTest {

    @Test
    @DisplayName("Should serialize an outbound api object to byte array and deserialize the byte array back")
    public void testSerializingAnOutboundApi() {
        final String testTopic = "test-outbound-Topic";
        final OutboundApiSerializer outboundApiSerializer = new OutboundApiSerializer();
        final GameStateOutbound.Api api = TennisOutboundApiFixture.create();
        final byte[] serializedOutboundApi = outboundApiSerializer.serialize(testTopic, api);

        final OutboundApiDeserializer outboundApiDeserializer = new OutboundApiDeserializer();
        final GameStateOutbound.Api deserializedApi = outboundApiDeserializer.deserialize(testTopic, serializedOutboundApi);

        assertEquals(api, deserializedApi);
    }

    @Test
    @DisplayName("Should serialize an outbound api object without topic to byte array and deserialize the byte array back")
    public void testSerializingAnOutboundApiWithoutTopic() {
        final OutboundApiSerializer outboundApiSerializer = new OutboundApiSerializer();
        final GameStateOutbound.Api api = TennisOutboundApiFixture.create();
        final byte[] serializedOutboundApi = outboundApiSerializer.serialize(api);

        final OutboundApiDeserializer outboundApiDeserializer = new OutboundApiDeserializer();
        final GameStateOutbound.Api deserializedApi = outboundApiDeserializer.deserialize(serializedOutboundApi);

        assertEquals(api, deserializedApi);
    }
}
