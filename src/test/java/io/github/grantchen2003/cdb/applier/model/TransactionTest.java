package io.github.grantchen2003.cdb.applier.model;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransactionTest {

    @Test
    void deserializeOperations_validJson_returnsOperations() {
        String json = """
            [
              {"opType":"PUT",    "table":"users",    "data":"{\\"id\\":1}"},
              {"opType":"DELETE", "table":"sessions", "data":"{\\"id\\":9}"}
            ]
            """;

        List<Operation> ops = Transaction.deserializeOperations(json);

        assertThat(ops).hasSize(2);

        assertThat(ops.get(0).opType()).isEqualTo(Operation.OpType.PUT);
        assertThat(ops.get(0).table()).isEqualTo("users");
        assertThat(ops.get(0).data()).isEqualTo("{\"id\":1}");

        assertThat(ops.get(1).opType()).isEqualTo(Operation.OpType.DELETE);
        assertThat(ops.get(1).table()).isEqualTo("sessions");
        assertThat(ops.get(1).data()).isEqualTo("{\"id\":9}");
    }

    @Test
    void deserializeOperations_emptyArray_returnsEmptyList() {
        List<Operation> ops = Transaction.deserializeOperations("[]");
        assertThat(ops).isEmpty();
    }

    @Test
    void deserializeOperations_invalidJson_throwsRuntimeException() {
        assertThatThrownBy(() -> Transaction.deserializeOperations("not-json"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to deserialize operation list");
    }

    @Test
    void constructor_storesFieldsCorrectly() {
        List<Operation> ops = List.of(new Operation(Operation.OpType.PUT, "t", "d"));
        Transaction tx = new Transaction(42L, ops);

        assertThat(tx.seqNum()).isEqualTo(42L);
        assertThat(tx.operations()).isEqualTo(ops);
    }
}