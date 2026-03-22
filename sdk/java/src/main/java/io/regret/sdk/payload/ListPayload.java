package io.regret.sdk.payload;

import com.fasterxml.jackson.databind.ObjectMapper;

public record ListPayload(String prefix) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static ListPayload fromBytes(byte[] payload) {
        try {
            return MAPPER.readValue(payload, ListPayload.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize ListPayload", e);
        }
    }
}
