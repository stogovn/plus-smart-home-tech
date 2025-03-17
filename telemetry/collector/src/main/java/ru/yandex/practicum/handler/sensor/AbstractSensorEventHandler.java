package ru.yandex.practicum.handler.sensor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.support.RetryTemplate;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Slf4j
public abstract class AbstractSensorEventHandler implements SensorEventHandler {

    protected final KafkaTemplate<String, Object> sensorKafkaTemplate;
    private final RetryTemplate retryTemplate;

    @Value("${sensorEventTopic}")
    protected String topic;

    // Конструктор с retryTemplate
    public AbstractSensorEventHandler(KafkaTemplate<String, Object> sensorKafkaTemplate, RetryTemplate retryTemplate) {
        this.sensorKafkaTemplate = sensorKafkaTemplate;
        this.retryTemplate = retryTemplate;
    }

    // Перегруженный конструктор без retryTemplate
    public AbstractSensorEventHandler(KafkaTemplate<String, Object> sensorKafkaTemplate) {
        this(sensorKafkaTemplate, null);
    }

    /**
     * Преобразует SensorEventProto в Avro-объект SensorEventAvro.
     */
    protected abstract SensorEventAvro mapToAvro(SensorEventProto eventProto);

    @Override
    public void handle(SensorEventProto eventProto) {
        SensorEventAvro avroEvent = mapToAvro(eventProto);
        log.info("Отправляю Avro-событие в Kafka: {}", avroEvent);
        try {
            retryTemplate.execute(context -> {
                // Используем get() для синхронного ожидания завершения отправки
                sensorKafkaTemplate.send(topic, avroEvent.getHubId(), avroEvent).get();
                return null;
            });
        } catch (Exception e) {
            log.error("Ошибка при отправке в Kafka", e);
        }
    }
}
