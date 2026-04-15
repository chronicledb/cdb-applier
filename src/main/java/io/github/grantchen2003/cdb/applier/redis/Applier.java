package io.github.grantchen2003.cdb.applier.redis;

import io.github.grantchen2003.cdb.applier.redis.model.Transaction;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

@Component
public class Applier implements Consumer<Transaction> {

    @Override
    public void accept(Transaction tx) {
    }
}
