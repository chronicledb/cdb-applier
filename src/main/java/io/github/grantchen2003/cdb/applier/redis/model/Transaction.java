package io.github.grantchen2003.cdb.applier.redis.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

public record Transaction(
        Long seqNum,
        List<Operation> operations
) {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static List<Operation> deserializeOperations(String json) {
        try {
            return MAPPER.readValue(json, new TypeReference<>() {});
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize operation list", e);
        }
    }
}