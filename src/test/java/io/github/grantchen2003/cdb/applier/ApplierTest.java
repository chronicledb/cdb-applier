package io.github.grantchen2003.cdb.applier;

import io.github.grantchen2003.cdb.applier.model.Operation;
import io.github.grantchen2003.cdb.applier.model.Transaction;
import io.github.grantchen2003.cdb.applier.writeschema.WriteSchemaService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;

import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ApplierTest {

    @Mock
    private RedisTemplate<String, String> redisTemplate;

    @Mock
    private WriteSchemaService writeSchemaService;

    @Mock
    private RedisOperations<String, String> redisOperations;

    @Mock
    private HashOperations<String, String, String> hashOperations;

    private RedisApplier applier;

    @BeforeEach
    void setUp() {
        applier = new RedisApplier(redisTemplate, writeSchemaService);

        lenient().when(redisOperations.opsForHash()).thenReturn((HashOperations) hashOperations);
        lenient().when(redisOperations.exec()).thenReturn(List.of());

        lenient().when(redisTemplate.execute(any(SessionCallback.class))).thenAnswer(invocation -> {
            SessionCallback<List<Object>> callback = invocation.getArgument(0);
            return callback.execute(redisOperations);
        });
    }

    @Test
    void applier_implementsConsumerOfTransaction() {
        assertThat(applier).isInstanceOf(Consumer.class);
    }

    @Test
    void accept_put_doesNotThrow() {
        when(writeSchemaService.getPrimaryKeyValue("accounts", "{\"id\":\"alice\"}")).thenReturn("alice");

        final Transaction tx = new Transaction(0L, List.of(
                new Operation(Operation.OpType.PUT, "accounts", "{\"id\":\"alice\"}")
        ));

        assertThatCode(() -> applier.accept(tx)).doesNotThrowAnyException();
        verify(hashOperations).put("accounts", "alice", "{\"id\":\"alice\"}");
    }

    @Test
    void accept_delete_doesNotThrow() {
        when(writeSchemaService.getPrimaryKeyValue("accounts", "{\"id\":\"alice\"}")).thenReturn("alice");

        final Transaction tx = new Transaction(1L, List.of(
                new Operation(Operation.OpType.DELETE, "accounts", "{\"id\":\"alice\"}")
        ));

        assertThatCode(() -> applier.accept(tx)).doesNotThrowAnyException();
        verify(hashOperations).delete("accounts", "alice");
    }

    @Test
    void accept_multipleOperations_executesAllInOrder() {
        when(writeSchemaService.getPrimaryKeyValue("accounts", "{\"id\":\"alice\"}")).thenReturn("alice");
        when(writeSchemaService.getPrimaryKeyValue("accounts", "{\"id\":\"bob\"}")).thenReturn("bob");

        final Transaction tx = new Transaction(2L, List.of(
                new Operation(Operation.OpType.PUT,    "accounts", "{\"id\":\"alice\"}"),
                new Operation(Operation.OpType.DELETE, "accounts", "{\"id\":\"bob\"}")
        ));

        assertThatCode(() -> applier.accept(tx)).doesNotThrowAnyException();
        verify(hashOperations).put("accounts", "alice", "{\"id\":\"alice\"}");
        verify(hashOperations).delete("accounts", "bob");
    }

    @Test
    void accept_emptyOperations_doesNotThrow() {
        final Transaction tx = new Transaction(3L, List.of());
        assertThatCode(() -> applier.accept(tx)).doesNotThrowAnyException();
        verifyNoInteractions(hashOperations);
    }
}