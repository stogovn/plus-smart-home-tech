package ru.yandex.practicum.service.handler.sensor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;

@Slf4j
@Component
public class SwitchSensorEventHandler implements SensorEventHandler {

    @Override
    public String getSensorType() {
        return SwitchSensorAvro.class.getName();
    }

    @Override
    public Integer getSensorValue(ConditionTypeAvro conditionType, SensorStateAvro sensorState) {
        SwitchSensorAvro switchSensor = (SwitchSensorAvro) sensorState.getData();
        return switch (conditionType) {
            case SWITCH -> switchSensor.getState() ? 1 : 0;
            default -> null;
        };
    }
}
