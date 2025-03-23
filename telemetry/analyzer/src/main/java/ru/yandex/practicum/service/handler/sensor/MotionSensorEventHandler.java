package ru.yandex.practicum.service.handler.sensor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;

@Slf4j
@Component
public class MotionSensorEventHandler implements SensorEventHandler {

    @Override
    public String getSensorType() {
        return MotionSensorAvro.class.getName();
    }

    @Override
    public Integer getSensorValue(ConditionTypeAvro conditionType, SensorStateAvro sensorState) {
        MotionSensorAvro motionSensor = (MotionSensorAvro) sensorState.getData();
        return switch (conditionType) {
            case MOTION -> motionSensor.getMotion() ? 1 : 0;
            default -> null;
        };
    }
}
