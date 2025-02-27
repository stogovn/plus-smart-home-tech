package ru.yandex.practicum.service.converter.hub;

import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.model.hub.ScenarioRemovedEvent;
import ru.yandex.practicum.service.converter.EventConverter;

public class ScenarioRemovedEventConverter extends EventConverter<ScenarioRemovedEvent, HubEventAvro> {
    @Override
    public HubEventAvro convert(ScenarioRemovedEvent event) {
        ScenarioRemovedEventAvro scenarioRemovedAvro = ScenarioRemovedEventAvro.newBuilder()
                .setName(event.getName())
                .build();
        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(scenarioRemovedAvro)
                .build();
    }
}