package ru.yandex.practicum.controller;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.service.AvroSerializer;
import ru.yandex.practicum.service.SensorEventDeserializer;

import java.util.Properties;

@Configuration
public class AggregatorConfig {
    @Bean
    public KafkaConsumer<String, SensorEventAvro> kafkaConsumer(
            @Value("${kafka.bootstrap-servers}") String kafkaServerAddress,
            @Value("${topic.telemetry.aggregator-client-id}") String aggregatorClientId,
            @Value("${topic.telemetry.aggregator-group-id}") String aggregatorGroupId
    ) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServerAddress);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, aggregatorGroupId);
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, aggregatorClientId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorEventDeserializer.class);
        // Дополнительные настройки для стабильности:
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(consumerProps);
    }

    @Bean
    public KafkaProducer<String, SensorsSnapshotAvro> kafkaProducer(
            @Value("${kafka.bootstrap-servers}") String kafkaServerAddress) {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServerAddress);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class);
        // Настройки для повторных попыток и таймаутов:
        producerProps.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        producerProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);
        return new KafkaProducer<>(producerProps);
    }
}
