package ru.yandex.practicum.service.converter.sensors;

import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.model.sensors.LightSensorEvent;
import ru.yandex.practicum.service.converter.EventConverter;

public class LightSensorEventConverter extends EventConverter<LightSensorEvent, SensorEventAvro> {

    @Override
    public SensorEventAvro convert(LightSensorEvent event) {
        LightSensorAvro lightAvro = LightSensorAvro.newBuilder()
                .setLinkQuality(event.getLinkQuality())
                .setLuminosity(event.getLuminosity())
                .build();
        return SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp().toEpochMilli())
                .setPayload(lightAvro)
                .build();
    }
}