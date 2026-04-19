package io.github.grantchen2003.cdb.applier.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OperationTest {

    @Test
    void record_storesAllFields() {
        Operation op = new Operation(Operation.OpType.PUT, "orders", "{\"id\":7}");

        assertThat(op.opType()).isEqualTo(Operation.OpType.PUT);
        assertThat(op.table()).isEqualTo("orders");
        assertThat(op.data()).isEqualTo("{\"id\":7}");
    }

    @Test
    void opType_hasPutAndDeleteVariants() {
        assertThat(Operation.OpType.values())
                .containsExactlyInAnyOrder(Operation.OpType.PUT, Operation.OpType.DELETE);
    }

    @Test
    void record_equality_basedOnFields() {
        Operation a = new Operation(Operation.OpType.DELETE, "users", "{}");
        Operation b = new Operation(Operation.OpType.DELETE, "users", "{}");

        assertThat(a).isEqualTo(b);
    }
}