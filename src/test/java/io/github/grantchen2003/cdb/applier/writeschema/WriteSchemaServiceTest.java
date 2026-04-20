package io.github.grantchen2003.cdb.applier.writeschema;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WriteSchemaServiceTest {

    private static final String WRITE_SCHEMA_JSON = """
            {
              "types": {
                "userId": { "variant": "text", "charset": "ascii", "size": [1, 64] }
              },
              "tables": {
                "accounts": {
                  "primaryKey": ["id"],
                  "requiredAttributes": ["years"],
                  "attributeTypes": { "id": "userId", "years": "age" },
                  "queryFamilies": []
                }
              }
            }
            """;

    private WriteSchemaService service;

    @BeforeEach
    void setUp() {
        service = new WriteSchemaService(WRITE_SCHEMA_JSON);
    }

    @Test
    void getPrimaryKeyValue_returnsCorrectValue() {
        String data = "{\"id\":\"alice\",\"years\":30}";
        assertEquals("alice", service.getPrimaryKeyValue("accounts", data));
    }

    @Test
    void getPrimaryKeyValue_worksWithNumericId() {
        String data = "{\"id\":42,\"years\":25}";
        assertEquals("42", service.getPrimaryKeyValue("accounts", data));
    }

    @Test
    void getPrimaryKeyValue_throwsWhenPrimaryKeyMissingFromData() {
        String data = "{\"years\":30}";
        assertThrows(RuntimeException.class, () -> service.getPrimaryKeyValue("accounts", data));
    }

    @Test
    void getPrimaryKeyValue_throwsWhenPrimaryKeyIsNull() {
        String data = "{\"id\":null,\"years\":30}";
        assertThrows(RuntimeException.class, () -> service.getPrimaryKeyValue("accounts", data));
    }

    @Test
    void getPrimaryKeyValue_throwsOnUnknownTable() {
        String data = "{\"id\":\"alice\"}";
        assertThrows(RuntimeException.class, () -> service.getPrimaryKeyValue("nonexistent", data));
    }

    @Test
    void getPrimaryKeyValue_throwsOnInvalidJson() {
        assertThrows(RuntimeException.class, () -> service.getPrimaryKeyValue("accounts", "not json"));
    }

    @Test
    void constructor_throwsOnInvalidWriteSchemaJson() {
        assertThrows(IllegalArgumentException.class, () -> new WriteSchemaService("bad json"));
    }
}