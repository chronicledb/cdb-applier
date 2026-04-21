package io.github.grantchen2003.cdb.applier;

import io.github.grantchen2003.cdb.applier.model.Operation;
import io.github.grantchen2003.cdb.applier.model.Transaction;
import io.github.grantchen2003.cdb.applier.writeschema.WriteSchemaService;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class RedisApplier implements Applier {

    private final RedisTemplate<String, String> redisTemplate;
    private final WriteSchemaService writeSchemaService;

    public RedisApplier(RedisTemplate<String, String> redisTemplate, WriteSchemaService writeSchemaService) {
        this.redisTemplate = redisTemplate;
        this.writeSchemaService = writeSchemaService;
    }

    @Override
    public void accept(Transaction tx) {
        final List<Object> results = redisTemplate.execute(new SessionCallback<>() {
            @Override
            public List<Object> execute(RedisOperations operations) throws DataAccessException {
                operations.multi();
                operations.opsForHash().increment("metadata", "seq_num", 1);
                for (final Operation op : tx.operations()) {
                    final String table = op.table();
                    final String primaryKey = writeSchemaService.getPrimaryKeyValue(op.table(), op.data());
                    switch (op.opType()) {
                        case PUT    -> operations.opsForHash().put(table, primaryKey, op.data());
                        case DELETE -> operations.opsForHash().delete(table, primaryKey);
                    }
                }
                return operations.exec();
            }
        });
    }
}
