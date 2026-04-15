package io.github.grantchen2003.cdb.applier.redis;

import io.github.grantchen2003.cdb.applier.redis.chronicleconsumer.ChronicleConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class ApplierRedisRunner implements ApplicationRunner {

    private final ChronicleConsumer chronicleConsumer;
    private final String chronicleId;

    public ApplierRedisRunner(
            ChronicleConsumer chronicleConsumer,
            @Value("${cdb.chronicle.chronicle-id}") String chronicleId) {
        this.chronicleConsumer = chronicleConsumer;
        this.chronicleId = chronicleId;
    }

    @Override
    public void run(ApplicationArguments args) {
        chronicleConsumer.consumeFromBeginning(chronicleId, (offset, message) -> {
            System.out.println("offset=" + offset + " message=" + message);
        });
    }
}