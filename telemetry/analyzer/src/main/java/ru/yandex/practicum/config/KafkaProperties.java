package ru.yandex.practicum.config;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.serialization.HubEventDeserializer;
import ru.yandex.practicum.serialization.SensorsSnapshotDeserializer;

import java.util.Properties;

@Setter
@Configuration
@RequiredArgsConstructor
public class KafkaProperties {

    @Getter
    private Properties hubEventProperties;
    @Getter
    private Properties snapshotsProperties;
    @Value("${kafka.bootstrapServers}")
    private String bootstrapServers;
    @Value("${kafka.commit}")
    private String autoCommitIntervalMs;
    @Getter
    @Value("${kafka.consumeAttemptTimeout}")
    private Long consumeAttemptTimeout;

    @PostConstruct
    public void init() {

        //hubEvent
        hubEventProperties = new Properties();
        hubEventProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        hubEventProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        hubEventProperties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitIntervalMs);
        hubEventProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        hubEventProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, HubEventDeserializer.class);
        hubEventProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "hub");

        //snapshotEvent
        snapshotsProperties = new Properties();
        snapshotsProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        snapshotsProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        snapshotsProperties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitIntervalMs);
        snapshotsProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        snapshotsProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorsSnapshotDeserializer.class);
        snapshotsProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "snapshots");

    }
}
