package ru.yandex.practicum.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.service.HubEventDeserializer;
import ru.yandex.practicum.service.SensorsSnapshotDeserializer;

import java.util.Properties;

@Configuration
public class AnalyzerConfig {
    @Bean
    public KafkaConsumer<String, HubEventAvro> kafkaHubConsumer(
            @Value("${kafka.hub-consumer.bootstrap-servers}") String kafkaServerAddress,
            @Value("${kafka.hub-consumer.group-id}") String groupId,
            @Value("${kafka.hub-consumer.client-id}") String clientId
    ) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServerAddress);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, HubEventDeserializer.class);
        return new KafkaConsumer<>(properties);
    }

    @Bean
    public KafkaConsumer<String, SensorsSnapshotAvro> kafkaSnapshotConsumer(
            @Value("${kafka.snapshot-consumer.bootstrap-servers}") String kafkaServerAddress,
            @Value("${kafka.snapshot-consumer.group-id}") String groupId,
            @Value("${kafka.snapshot-consumer.client-id}") String clientId
    ) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServerAddress);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorsSnapshotDeserializer.class);
        return new KafkaConsumer<>(properties);
    }
}
