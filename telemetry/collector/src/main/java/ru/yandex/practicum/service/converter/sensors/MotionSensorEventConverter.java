package ru.yandex.practicum.service.converter.sensors;

import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.model.sensors.MotionSensorEvent;
import ru.yandex.practicum.service.converter.EventConverter;

public class MotionSensorEventConverter extends EventConverter<MotionSensorEvent, SensorEventAvro> {

    @Override
    public SensorEventAvro convert(MotionSensorEvent event) {
        MotionSensorAvro motionAvro = MotionSensorAvro.newBuilder()
                .setLinkQuality(event.getLinkQuality())
                .setMotion(event.isMotion())
                .setVoltage(event.getVoltage())
                .build();
        return SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp().toEpochMilli())
                .setPayload(motionAvro)
                .build();
    }
}