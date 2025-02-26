package ru.yandex.practicum.service.converter.sensors;

import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
import ru.yandex.practicum.model.sensors.TemperatureSensorEvent;
import ru.yandex.practicum.service.converter.EventConverter;

public class TemperatureSensorEventConverter extends EventConverter<TemperatureSensorEvent, SensorEventAvro> {

    @Override
    public SensorEventAvro convert(TemperatureSensorEvent event) {
        TemperatureSensorAvro tempAvro = TemperatureSensorAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp().toEpochMilli())
                .setTemperatureC(event.getTemperatureC())
                .setTemperatureF(event.getTemperatureF())
                .build();
        return SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp().toEpochMilli())
                .setPayload(tempAvro)
                .build();
    }
}