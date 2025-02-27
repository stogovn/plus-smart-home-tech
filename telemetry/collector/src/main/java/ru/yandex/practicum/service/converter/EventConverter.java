package ru.yandex.practicum.service.converter;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.kafka.core.KafkaTemplate;

@Slf4j
public abstract class EventConverter<T, A extends SpecificRecordBase> {

    /**
     * Преобразует доменную модель в соответствующий Avro-объект.
     */
    public abstract A convert(T event);

    /**
     * Отправляет преобразованное событие в Kafka.
     */
    public void sendEvent(KafkaTemplate<String, Object> kafkaTemplate, String topic, String key, T event) {
        log.info("Обрабатываю событие: {}", event);
        A avroEvent = convert(event);
        log.info("Отправляю Avro-событие в Kafka: {}", avroEvent);
        kafkaTemplate.send(topic, key, avroEvent);
    }
}