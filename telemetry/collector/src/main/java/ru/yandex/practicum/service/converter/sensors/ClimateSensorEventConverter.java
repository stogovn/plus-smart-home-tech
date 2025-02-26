package ru.yandex.practicum.service.converter.sensors;

import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.model.sensors.ClimateSensorEvent;
import ru.yandex.practicum.service.converter.EventConverter;

public class ClimateSensorEventConverter extends EventConverter<ClimateSensorEvent, SensorEventAvro> {

    @Override
    public SensorEventAvro convert(ClimateSensorEvent event) {
        ClimateSensorAvro climateAvro = ClimateSensorAvro.newBuilder()
                .setTemperatureC(event.getTemperatureC())
                .setHumidity(event.getHumidity())
                .setCo2Level(event.getCo2Level())
                .build();
        return SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp().toEpochMilli())
                .setPayload(climateAvro)
                .build();
    }
}