package ru.yandex.practicum.service.handler.sensor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;

@Slf4j
@Component
public class TemperatureSensorEventHandler implements SensorEventHandler {

    @Override
    public String getSensorType() {
        return TemperatureSensorAvro.class.getName();
    }

    @Override
    public Integer getSensorValue(ConditionTypeAvro conditionType, SensorStateAvro sensorState) {
        TemperatureSensorAvro temperatureSensor = (TemperatureSensorAvro) sensorState.getData();
        return switch (conditionType) {
            case TEMPERATURE -> temperatureSensor.getTemperatureC();
            default -> null;
        };
    }
}
