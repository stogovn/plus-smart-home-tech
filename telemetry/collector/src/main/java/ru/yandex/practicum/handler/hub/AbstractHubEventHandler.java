package ru.yandex.practicum.handler.hub;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

@Slf4j
@NoArgsConstructor(force = true)
public abstract class AbstractHubEventHandler implements HubEventHandler {

    protected final KafkaTemplate<String, Object> hubKafkaTemplate;

    @Value("${hubEventTopic}")
    protected String topic;

    /**
     * Преобразует HubEventProto в Avro-объект HubEventAvro.
     */
    protected abstract HubEventAvro mapToAvro(HubEventProto eventProto);

    @Override
    public void handle(HubEventProto eventProto) {
        HubEventAvro avroEvent = mapToAvro(eventProto);
        log.info("Отправляю Avro-событие в Kafka: {}", avroEvent);
        assert hubKafkaTemplate != null;
        hubKafkaTemplate.send(topic, avroEvent.getHubId(), avroEvent);
    }
}
