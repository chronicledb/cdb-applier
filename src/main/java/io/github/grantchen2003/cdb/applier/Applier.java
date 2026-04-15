package io.github.grantchen2003.cdb.applier;

import io.github.grantchen2003.cdb.applier.model.Transaction;
import java.util.function.Consumer;

public interface Applier extends Consumer<Transaction> {
    @Override
    void accept(Transaction tx);
}