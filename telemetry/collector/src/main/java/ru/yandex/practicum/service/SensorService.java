package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.model.sensors.SensorEvent;

@Service
@RequiredArgsConstructor
public class SensorService {

    private final KafkaProducerService kafkaProducerService;

    public void processSensorEvent(SensorEvent event) {
        kafkaProducerService.sendSensorEvent(event);
    }

}