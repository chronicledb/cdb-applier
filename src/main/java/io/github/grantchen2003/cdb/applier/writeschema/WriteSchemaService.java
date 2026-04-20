package io.github.grantchen2003.cdb.applier.writeschema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class WriteSchemaService {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final WriteSchema writeSchema;

    public WriteSchemaService(@Value("${cdb.chronicle.write-schema-json}") String writeSchemaJson) {
        this.writeSchema = WriteSchema.fromJson(writeSchemaJson);
    }

    public String getPrimaryKeyValue(String tableName, String jsonData) {
        final String primaryKeyName = getPrimaryKeyName(tableName);
        try {
            final JsonNode root = MAPPER.readTree(jsonData);
            final JsonNode pkNode = root.get(primaryKeyName);

            if (pkNode == null || pkNode.isNull()) {
                throw new IllegalArgumentException(String.format(
                        "Primary key '%s' not found in data for table '%s'", primaryKeyName, tableName));
            }

            return pkNode.asText();
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract primary key value", e);
        }
    }

    private String getPrimaryKeyName(String tableName) {
        final WriteSchema.TableDefinition tableMetadata = writeSchema.tables().get(tableName);
        if (tableMetadata == null) {
            throw new IllegalArgumentException("Unknown table: " + tableName);
        }
        return tableMetadata.primaryKey().get(0);
    }
}
