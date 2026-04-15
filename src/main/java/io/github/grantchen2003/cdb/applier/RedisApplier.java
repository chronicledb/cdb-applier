package io.github.grantchen2003.cdb.applier;

import io.github.grantchen2003.cdb.applier.model.Transaction;
import org.springframework.stereotype.Component;

@Component
public class RedisApplier implements Applier {
    @Override
    public void accept(Transaction tx) {
    }
}
