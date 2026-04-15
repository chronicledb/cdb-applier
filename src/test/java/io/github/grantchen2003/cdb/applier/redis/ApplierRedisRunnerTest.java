package io.github.grantchen2003.cdb.applier.redis;

import io.github.grantchen2003.cdb.applier.redis.chronicleconsumer.ChronicleConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ApplierRedisRunnerTest {

    @Mock
    private ChronicleConsumer mockConsumer;

    @Test
    void testRunnerTriggersConsumer() {
        String testId = "test-chronicle";
        ApplierRedisRunner runner = new ApplierRedisRunner(mockConsumer, testId);

        runner.run(null);

        verify(mockConsumer, times(1)).consumeFromBeginning(eq(testId), any());
    }
}