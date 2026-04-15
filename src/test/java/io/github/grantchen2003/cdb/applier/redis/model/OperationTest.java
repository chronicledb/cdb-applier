package io.github.grantchen2003.cdb.applier.redis.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OperationTest {

    @Test
    void record_storesAllFields() {
        Operation op = new Operation(Operation.OpType.SET, "orders", "{\"id\":7}");

        assertThat(op.opType()).isEqualTo(Operation.OpType.SET);
        assertThat(op.table()).isEqualTo("orders");
        assertThat(op.data()).isEqualTo("{\"id\":7}");
    }

    @Test
    void opType_hasSetAndDeleteVariants() {
        assertThat(Operation.OpType.values())
                .containsExactlyInAnyOrder(Operation.OpType.SET, Operation.OpType.DELETE);
    }

    @Test
    void record_equality_basedOnFields() {
        Operation a = new Operation(Operation.OpType.DELETE, "users", "{}");
        Operation b = new Operation(Operation.OpType.DELETE, "users", "{}");

        assertThat(a).isEqualTo(b);
    }
}