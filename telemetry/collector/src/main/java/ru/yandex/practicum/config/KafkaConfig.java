package ru.yandex.practicum.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;

import org.springframework.kafka.core.KafkaTemplate;
import ru.yandex.practicum.service.AvroSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${kafkaBootstrapServer}")
    private String bootstrapServers;

    private Map<String, Object> commonProducerConfigs() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class);
        return configProps;
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        return new DefaultKafkaProducerFactory<>(commonProducerConfigs());
    }

    @Bean("sensorKafkaTemplate")
    public KafkaTemplate<String, Object> sensorKafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean("hubKafkaTemplate")
    public KafkaTemplate<String, Object> hubKafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}