package io.github.grantchen2003.cdb.applier;

import io.github.grantchen2003.cdb.applier.chronicle.ChronicleSubscriber;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class ApplierRunner implements ApplicationRunner {

    private final Applier applier;
    private final ChronicleSubscriber chronicleSubscriber;
    private final String chronicleId;

    public ApplierRunner(
            Applier applier,
            ChronicleSubscriber chronicleSubscriber,
            @Value("${cdb.chronicle.chronicle-id}") String chronicleId) {
        this.applier = applier;
        this.chronicleSubscriber = chronicleSubscriber;
        this.chronicleId = chronicleId;
    }

    @Override
    public void run(ApplicationArguments args) {
        chronicleSubscriber.subscribeFromStart(chronicleId, applier);
    }
}