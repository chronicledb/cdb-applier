package io.github.grantchen2003.cdb.applier;

import io.github.grantchen2003.cdb.applier.model.Operation;
import io.github.grantchen2003.cdb.applier.model.Transaction;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class ApplierTest {

    private final Applier applier = new RedisApplier();

    @Test
    void applier_implementsConsumerOfTransaction() {
        assertThat(applier).isInstanceOf(Consumer.class);
    }

    @Test
    void accept_doesNotThrow() {
        Transaction tx = new Transaction(1L, List.of(
                new Operation(Operation.OpType.SET, "users", "{\"id\":1}")
        ));

        assertThatCode(() -> applier.accept(tx)).doesNotThrowAnyException();
    }

    @Test
    void accept_emptyOperations_doesNotThrow() {
        Transaction tx = new Transaction(2L, List.of());
        assertThatCode(() -> applier.accept(tx)).doesNotThrowAnyException();
    }
}