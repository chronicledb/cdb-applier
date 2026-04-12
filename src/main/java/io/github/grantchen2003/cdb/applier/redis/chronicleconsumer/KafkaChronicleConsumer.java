package io.github.grantchen2003.cdb.applier.redis.chronicleconsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class KafkaChronicleConsumer implements ChronicleConsumer {

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(2);

    private final String bootstrapServers;

    public KafkaChronicleConsumer(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    @Override
    public void consumeFromBeginning(String chronicleId, BiConsumer<Long, String> onMessage) {
        try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getKafkaConsumerProperties())) {
            final List<TopicPartition> partitions = consumer
                    .partitionsFor(chronicleId)
                    .stream()
                    .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                    .collect(Collectors.toList());

            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);

                for (final ConsumerRecord<String, String> record : records) {
                    onMessage.accept(Long.parseLong(record.key()), record.value());
                }
            }
        }
    }

    private Properties getKafkaConsumerProperties() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }
}
