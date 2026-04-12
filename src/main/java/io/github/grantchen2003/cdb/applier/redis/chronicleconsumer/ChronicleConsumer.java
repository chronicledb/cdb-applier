package io.github.grantchen2003.cdb.applier.redis.chronicleconsumer;

import java.util.function.BiConsumer;

public interface ChronicleConsumer {
    void consumeFromBeginning(String chronicleId, BiConsumer<Long, String> onMessage);
}