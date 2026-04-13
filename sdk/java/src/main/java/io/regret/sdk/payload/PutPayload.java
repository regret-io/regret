package io.regret.sdk.payload;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

public record PutPayload(
        String key,
        String value,
        boolean ephemeral,
        boolean sequence,
        String prefix,
        Long delta,
        @JsonProperty("index_name") String indexName,
        @JsonProperty("index_key") String indexKey
) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static PutPayload fromBytes(byte[] payload) {
        try {
            return MAPPER.readValue(payload, PutPayload.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize PutPayload", e);
        }
    }

    public byte[] toBytes() {
        try {
            return MAPPER.writeValueAsBytes(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize PutPayload", e);
        }
    }
}
