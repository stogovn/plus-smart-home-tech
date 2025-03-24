package ru.yandex.practicum.service.handler.sensor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;

@Slf4j
@Component
public class LightSensorEventHandler implements SensorEventHandler {

    @Override
    public String getSensorType() {
        return LightSensorAvro.class.getName();
    }

    @Override
    public Integer getSensorValue(ConditionTypeAvro conditionType, SensorStateAvro sensorState) {
        LightSensorAvro lightSensor = (LightSensorAvro) sensorState.getData();
        return switch (conditionType) {
            case LUMINOSITY -> lightSensor.getLuminosity();
            default -> null;
        };
    }
}
