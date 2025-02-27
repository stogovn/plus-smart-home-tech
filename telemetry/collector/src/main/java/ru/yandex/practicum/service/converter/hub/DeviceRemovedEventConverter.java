package ru.yandex.practicum.service.converter.hub;

import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.model.hub.DeviceRemovedEvent;
import ru.yandex.practicum.service.converter.EventConverter;

public class DeviceRemovedEventConverter extends EventConverter<DeviceRemovedEvent, HubEventAvro> {
    @Override
    public HubEventAvro convert(DeviceRemovedEvent event) {
        DeviceRemovedEventAvro deviceRemovedAvro = DeviceRemovedEventAvro.newBuilder()
                .setId(event.getId())
                .build();
        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(deviceRemovedAvro)
                .build();
    }
}