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
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServerAddress);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, aggregatorGroupId);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, aggregatorClientId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorEventDeserializer.class);
        return new KafkaConsumer<>(properties);
    }

    @Bean
    public KafkaProducer<String, SensorsSnapshotAvro> kafkaProducer(
            @Value("${kafka.bootstrap-servers}") String kafkaServerAddress) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServerAddress);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class);
        return new KafkaProducer<>(properties);
    }
}
