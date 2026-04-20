package io.github.grantchen2003.cdb.applier.writeschema;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteSchemaTest {

    private static final String VALID_JSON = """
            {
              "types": {
                "userId": { "variant": "text", "charset": "ascii", "size": [1, 64] },
                "age":    { "variant": "integer", "range": [0, 150] },
                "balance":{ "variant": "decimal", "precision": 18, "scale": [0, 2] },
                "role":   { "variant": "enum", "values": ["admin", "user", "guest"] }
              },
              "tables": {
                "accounts": {
                  "primaryKey": ["id"],
                  "requiredAttributes": ["years"],
                  "attributeTypes": {
                    "id":      "userId",
                    "years":   "age",
                    "credits": "balance",
                    "access":  "role"
                  },
                  "queryFamilies": [["access"], ["access", "years"]]
                }
              }
            }
            """;

    @Test
    void fromJson_parsesTypesCorrectly() {
        WriteSchema schema = WriteSchema.fromJson(VALID_JSON);

        assertEquals(4, schema.types().size());

        WriteSchema.TypeDefinition userId = schema.types().get("userId");
        assertEquals("text", userId.variant());
        assertEquals("ascii", userId.charset());
        assertEquals(1L, userId.size().get(0));
        assertEquals(64L, userId.size().get(1));

        WriteSchema.TypeDefinition age = schema.types().get("age");
        assertEquals("integer", age.variant());
        assertEquals(0L, age.range().get(0));
        assertEquals(150L, age.range().get(1));

        WriteSchema.TypeDefinition balance = schema.types().get("balance");
        assertEquals("decimal", balance.variant());
        assertEquals(18L, balance.precision());
        assertEquals(0L, balance.scale().get(0));
        assertEquals(2L, balance.scale().get(1));

        WriteSchema.TypeDefinition role = schema.types().get("role");
        assertEquals("enum", role.variant());
        assertEquals(3, role.values().size());
        assertTrue(role.values().contains("admin"));
        assertTrue(role.values().contains("user"));
        assertTrue(role.values().contains("guest"));
    }

    @Test
    void fromJson_parsesTablesCorrectly() {
        WriteSchema schema = WriteSchema.fromJson(VALID_JSON);

        assertTrue(schema.tables().containsKey("accounts"));
        WriteSchema.TableDefinition accounts = schema.tables().get("accounts");

        assertEquals(List.of("id"), accounts.primaryKey());
        assertEquals(List.of("years"), accounts.requiredAttributes());
        assertEquals("userId", accounts.attributeTypes().get("id"));
        assertEquals("age",    accounts.attributeTypes().get("years"));
        assertEquals("balance",accounts.attributeTypes().get("credits"));
        assertEquals("role",   accounts.attributeTypes().get("access"));
        assertEquals(2, accounts.queryFamilies().size());
    }

    @Test
    void fromJson_nullableFieldsAreNullWhenAbsent() {
        WriteSchema schema = WriteSchema.fromJson(VALID_JSON);

        WriteSchema.TypeDefinition age = schema.types().get("age");
        assertNull(age.charset());
        assertNull(age.size());
        assertNull(age.values());
        assertNull(age.precision());
        assertNull(age.scale());
    }

    @Test
    void fromJson_throwsOnInvalidJson() {
        assertThrows(IllegalArgumentException.class, () -> WriteSchema.fromJson("not json"));
    }

    @Test
    void fromJson_throwsOnEmptyString() {
        assertThrows(IllegalArgumentException.class, () -> WriteSchema.fromJson(""));
    }
}