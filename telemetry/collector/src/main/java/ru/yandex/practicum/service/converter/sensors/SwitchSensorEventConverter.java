package ru.yandex.practicum.service.converter.sensors;

import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.model.sensors.SwitchSensorEvent;
import ru.yandex.practicum.service.converter.EventConverter;

public class SwitchSensorEventConverter extends EventConverter<SwitchSensorEvent, SensorEventAvro> {

    @Override
    public SensorEventAvro convert(SwitchSensorEvent event) {
        SwitchSensorAvro switchAvro = SwitchSensorAvro.newBuilder()
                .setState(event.isState())
                .build();
        return SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp().toEpochMilli())
                .setPayload(switchAvro)
                .build();
    }
}