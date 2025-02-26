package ru.yandex.practicum.service.converter.hub;

import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.model.hub.DeviceAddedEvent;
import ru.yandex.practicum.service.converter.EventConverter;

public class DeviceAddedEventConverter extends EventConverter<DeviceAddedEvent, HubEventAvro> {
    @Override
    public HubEventAvro convert(DeviceAddedEvent event) {
        DeviceAddedEventAvro deviceAddedAvro = DeviceAddedEventAvro.newBuilder()
                .setId(event.getId())
                .setType(DeviceTypeAvro.valueOf(event.getDeviceType().name()))
                .build();
        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(deviceAddedAvro)
                .build();
    }
}