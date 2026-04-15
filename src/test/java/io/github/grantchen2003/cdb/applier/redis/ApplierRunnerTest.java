package io.github.grantchen2003.cdb.applier.redis;

import io.github.grantchen2003.cdb.applier.redis.chronicle.ChronicleSubscriber;
import io.github.grantchen2003.cdb.applier.redis.model.Transaction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.ApplicationArguments;

import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ApplierRunnerTest {

    @Mock private Applier applier;
    @Mock private ChronicleSubscriber chronicleSubscriber;
    @Mock private ApplicationArguments args;

    private ApplierRunner runner(String chronicleId) {
        return new ApplierRunner(applier, chronicleSubscriber, chronicleId);
    }

    @Test
    void run_callsSubscribeFromStartWithCorrectChronicleId() {
        runner("my-chronicle").run(args);

        verify(chronicleSubscriber).subscribeFromStart(eq("my-chronicle"), any());
    }

    @Test
    void run_passesApplierAsMessageHandler() {
        runner("any-id").run(args);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Consumer<Transaction>> captor = ArgumentCaptor.forClass(Consumer.class);
        verify(chronicleSubscriber).subscribeFromStart(any(), captor.capture());

        assertThat(captor.getValue()).isSameAs(applier);
    }

    @Test
    void run_callsSubscribeExactlyOnce() {
        runner("id").run(args);
        verify(chronicleSubscriber, times(1)).subscribeFromStart(any(), any());
    }
}