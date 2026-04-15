package io.github.grantchen2003.cdb.applier.chronicle;

import io.github.grantchen2003.cdb.applier.model.Transaction;

import java.util.function.Consumer;

public interface ChronicleSubscriber {
    void subscribeFromStart(String chronicleId, Consumer<Transaction> onMessage);
}