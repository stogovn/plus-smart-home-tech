package ru.yandex.practicum.service.handler.sensor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;

@Slf4j
@Component
public class ClimateSensorEventHandler implements SensorEventHandler {

    @Override
    public String getSensorType() {
        return ClimateSensorAvro.class.getName();
    }

    @Override
    public Integer getSensorValue(ConditionTypeAvro conditionType, SensorStateAvro sensorState) {
        ClimateSensorAvro climateSensor = (ClimateSensorAvro) sensorState.getData();
        return switch (conditionType) {
            case TEMPERATURE -> climateSensor.getTemperatureC();
            case CO2LEVEL -> climateSensor.getCo2Level();
            case HUMIDITY -> climateSensor.getHumidity();
            default -> null;
        };
    }
}
