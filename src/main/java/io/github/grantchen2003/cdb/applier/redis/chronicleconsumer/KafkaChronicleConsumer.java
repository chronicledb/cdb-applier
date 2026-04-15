package io.github.grantchen2003.cdb.applier.redis.chronicleconsumer;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

import java.util.function.BiConsumer;

@Component
public class KafkaChronicleConsumer implements ChronicleConsumer {
    private final ConsumerFactory<String, String> consumerFactory;

    public KafkaChronicleConsumer(ConsumerFactory<String, String> consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    @Override
    public void consumeFromBeginning(String id, BiConsumer<Long, String> onMsg) {
        final ContainerProperties props = new ContainerProperties(id);
        props.setMessageListener((MessageListener<String, String>) record ->
                onMsg.accept(Long.parseLong(record.key()), record.value()));
        new KafkaMessageListenerContainer<>(consumerFactory, props).start();
    }
}