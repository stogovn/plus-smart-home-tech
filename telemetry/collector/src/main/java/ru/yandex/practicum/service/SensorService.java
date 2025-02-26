package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.model.sensors.ClimateSensorEvent;
import ru.yandex.practicum.model.sensors.LightSensorEvent;
import ru.yandex.practicum.model.sensors.MotionSensorEvent;
import ru.yandex.practicum.model.sensors.SensorEvent;
import ru.yandex.practicum.model.sensors.SwitchSensorEvent;
import ru.yandex.practicum.model.sensors.TemperatureSensorEvent;
import ru.yandex.practicum.service.converter.sensors.ClimateSensorEventConverter;
import ru.yandex.practicum.service.converter.sensors.LightSensorEventConverter;
import ru.yandex.practicum.service.converter.sensors.MotionSensorEventConverter;
import ru.yandex.practicum.service.converter.sensors.SwitchSensorEventConverter;
import ru.yandex.practicum.service.converter.sensors.TemperatureSensorEventConverter;

@Slf4j
@Service
@RequiredArgsConstructor
public class SensorService {

    private final KafkaTemplate<String, Object> sensorKafkaTemplate;

    @Value("${sensorEventTopic}")
    private String sensorEventTopic;

    public void processSensorEvent(SensorEvent event) {
        switch (event.getType()) {
            case CLIMATE_SENSOR_EVENT:
                new ClimateSensorEventConverter()
                        .sendEvent(sensorKafkaTemplate, sensorEventTopic, event.getHubId(), (ClimateSensorEvent) event);
                break;
            case LIGHT_SENSOR_EVENT:
                new LightSensorEventConverter()
                        .sendEvent(sensorKafkaTemplate, sensorEventTopic, event.getHubId(), (LightSensorEvent) event);
                break;
            case MOTION_SENSOR_EVENT:
                new MotionSensorEventConverter()
                        .sendEvent(sensorKafkaTemplate, sensorEventTopic, event.getHubId(), (MotionSensorEvent) event);
                break;
            case SWITCH_SENSOR_EVENT:
                new SwitchSensorEventConverter()
                        .sendEvent(sensorKafkaTemplate, sensorEventTopic, event.getHubId(), (SwitchSensorEvent) event);
                break;
            case TEMPERATURE_SENSOR_EVENT:
                new TemperatureSensorEventConverter()
                        .sendEvent(sensorKafkaTemplate, sensorEventTopic, event.getHubId(), (TemperatureSensorEvent) event);
                break;
            default:
                throw new IllegalArgumentException("Неизвестный тип события sensor: " + event.getType());
        }
    }
}