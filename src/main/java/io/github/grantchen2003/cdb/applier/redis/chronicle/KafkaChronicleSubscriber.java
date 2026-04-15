package io.github.grantchen2003.cdb.applier.redis.chronicle;

import io.github.grantchen2003.cdb.applier.redis.model.Operation;
import io.github.grantchen2003.cdb.applier.redis.model.Transaction;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.function.Consumer;

@Component
public class KafkaChronicleSubscriber implements ChronicleSubscriber {
    private final ConsumerFactory<String, String> consumerFactory;

    public KafkaChronicleSubscriber(ConsumerFactory<String, String> consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    @Override
    public void subscribeFromStart(String id, Consumer<Transaction> onMsg) {
        final ContainerProperties props = new ContainerProperties(id);

        props.setMessageListener((MessageListener<String, String>) record -> {
            final long seqNum = Long.parseLong(record.key());
            final List<Operation> operations = Transaction.deserializeOperations(record.value());
            onMsg.accept(new Transaction(seqNum, operations));
        });

        new KafkaMessageListenerContainer<>(consumerFactory, props).start();
    }
}