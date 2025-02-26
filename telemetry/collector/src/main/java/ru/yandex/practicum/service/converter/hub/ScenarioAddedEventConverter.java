package ru.yandex.practicum.service.converter.hub;

import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.model.hub.DeviceAction;
import ru.yandex.practicum.model.hub.ScenarioAddedEvent;
import ru.yandex.practicum.model.hub.ScenarioCondition;
import ru.yandex.practicum.service.converter.EventConverter;

import java.util.List;

public class ScenarioAddedEventConverter extends EventConverter<ScenarioAddedEvent, HubEventAvro> {

    @Override
    public HubEventAvro convert(ScenarioAddedEvent event) {
        List<ScenarioConditionAvro> avroConditions = event.getConditions().stream()
                .map(this::mapToAvroScenarioCondition)
                .toList();
        List<DeviceActionAvro> avroActions = event.getActions().stream()
                .map(this::mapToAvroDeviceAction)
                .toList();
        ScenarioAddedEventAvro scenarioAddedAvro = ScenarioAddedEventAvro.newBuilder()
                .setName(event.getName())
                .setConditions(avroConditions)
                .setActions(avroActions)
                .build();
        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(scenarioAddedAvro)
                .build();
    }

    private ScenarioConditionAvro mapToAvroScenarioCondition(ScenarioCondition condition) {
        return ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
                .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()))
                .setValue(condition.getValue())
                .build();
    }

    private DeviceActionAvro mapToAvroDeviceAction(DeviceAction action) {
        return DeviceActionAvro.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(ActionTypeAvro.valueOf(action.getType().name()))
                .setValue(action.getValue())
                .build();
    }
}