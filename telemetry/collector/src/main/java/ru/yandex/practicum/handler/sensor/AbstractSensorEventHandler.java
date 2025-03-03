package ru.yandex.practicum.handler.sensor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Slf4j
@RequiredArgsConstructor
public abstract class AbstractSensorEventHandler implements SensorEventHandler {

    protected final KafkaTemplate<String, Object> sensorKafkaTemplate;

    @Value("${sensorEventTopic}")
    protected String topic;

    /**
     * Преобразует SensorEventProto в Avro-объект SensorEventAvro.
     */
    protected abstract SensorEventAvro mapToAvro(SensorEventProto eventProto);

    @Override
    public void handle(SensorEventProto eventProto) {
        SensorEventAvro avroEvent = mapToAvro(eventProto);
        log.info("Отправляю Avro-событие в Kafka: {}", avroEvent);
        assert sensorKafkaTemplate != null;
        sensorKafkaTemplate.send(topic, avroEvent.getHubId(), avroEvent);
    }
}
