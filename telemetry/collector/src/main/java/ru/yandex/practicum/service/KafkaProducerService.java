package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.model.sensors.SensorEvent;
import ru.yandex.practicum.model.hub.HubEvent;

@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<String, Object> sensorKafkaTemplate;
    private final KafkaTemplate<String, Object> hubKafkaTemplate;
    @Value("${sensorEventTopic}")
    private String sensorEventTopic;

    @Value("${hubEventTopic}")
    private String hubEventTopic;


    public void sendSensorEvent(SensorEvent event) {
        sensorKafkaTemplate.send(sensorEventTopic, event.getId(), event);
    }

    public void sendHubEvent(HubEvent event) {
        hubKafkaTemplate.send(hubEventTopic, event.getHubId(), event);
    }
}