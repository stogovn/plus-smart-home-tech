package ru.yandex.practicum.service.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.model.Action;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.model.Sensor;
import ru.yandex.practicum.model.dto.ActionDto;
import ru.yandex.practicum.model.dto.ActionSensorConditionDto;
import ru.yandex.practicum.model.dto.ConditionDto;
import ru.yandex.practicum.repository.persister.ActionConditionPersister;
import ru.yandex.practicum.repository.persister.SensorPersister;
import ru.yandex.practicum.service.HubService;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class EventService implements HubService<HubEventAvro> {

    private final SensorPersister sensorPersister;
    private final ActionConditionPersister actionConditionPersister;


    @Override
    public void process(HubEventAvro event) {
        String hubId = event.getHubId();
        Object payload = event.getPayload();

        if (payload instanceof DeviceAddedEventAvro deviceAddedEventAvro) {
            handleDeviceAddedEvent(deviceAddedEventAvro, hubId);
            return;
        }
        if (payload instanceof DeviceRemovedEventAvro deviceRemovedEventAvro) {
            handleDeviceRemovedEvent(deviceRemovedEventAvro, hubId);
            return;
        }

        if (payload instanceof ScenarioAddedEventAvro scenarioAddedEvent) {
            handleScenarioAddedEvent(scenarioAddedEvent, hubId);
            return;
        }
        if (payload instanceof ScenarioRemovedEventAvro scenarioRemovedEvent) {
            handleScenarioAddedEvent(scenarioRemovedEvent, hubId);
        }
    }


    private void handleDeviceAddedEvent(DeviceAddedEventAvro event, String hubId) {
        Sensor sensor = new Sensor();
        sensor.setHubId(hubId);
        sensor.setId(event.getId());
        sensorPersister.save(sensor);
    }

    private void handleDeviceRemovedEvent(DeviceRemovedEventAvro event, String hubId) {
        Sensor sensor = new Sensor();
        sensor.setHubId(hubId);
        sensorPersister.removeById(event.getId());
    }

    private void handleScenarioAddedEvent(ScenarioAddedEventAvro event, String hubId) {
        List<DeviceActionAvro> actionsAvro = event.getActions();
        List<ScenarioConditionAvro> conditionsAvro = event.getConditions();

        List<String> sensorsIds = actionsAvro.stream()
                .map(DeviceActionAvro::getSensorId)
                .toList();
        List<Sensor> sensors = sensorPersister.getSensorsByIds(sensorsIds);

        Map<Long, ActionDto> actionsWithSensorIds = actionsAvro.stream().map(actionAvro -> new ActionDto(
                Long.parseLong(actionAvro.getSensorId()),
                actionAvro.getType().name(),
                actionAvro.getValue()
        )).collect(Collectors.toMap(ActionDto::getSensorId, Function.identity()));
        Map<Long, ConditionDto> conditionsWithSensorIds = conditionsAvro.stream()
                .map(conditionAvro -> {
                    int value;
                    if (conditionAvro.getValue() instanceof Boolean) {
                        boolean isTrue = ((Boolean) conditionAvro.getValue());
                        if (isTrue) {
                            value = 1;
                        } else {
                            value = 0;
                        }
                    } else {
                        value = (int) conditionAvro.getValue();
                    }
                    return ConditionDto.builder()
                            .sensorId(Long.parseLong(conditionAvro.getSensorId()))
                            .operation(conditionAvro.getOperation().name())
                            .type(conditionAvro.getType().name())
                            .value(value)
                            .build();
                }).collect(Collectors.toMap(ConditionDto::getSensorId, Function.identity()));

        List<ActionSensorConditionDto> dtos = sensors.stream()
                .map(sensor -> {
                    ActionDto actionDto = actionsWithSensorIds.get(Long.parseLong(sensor.getId()));
                    ConditionDto conditionDto = conditionsWithSensorIds.get(Long.parseLong(sensor.getId()));

                    Action action = new Action();
                    action.setValue(actionDto.getValue());
                    action.setType(actionDto.getType());

                    Condition condition = new Condition();
                    condition.setOperation(conditionDto.getOperation());
                    condition.setType(conditionDto.getType());
                    condition.setValue(conditionDto.getValue());

                    return new ActionSensorConditionDto(
                            condition,
                            sensor,
                            action
                    );
                }).toList();
        Scenario scenario = new Scenario();
        scenario.setHubId(hubId);
        scenario.setName(event.getName());
        Map<Scenario, List<ActionSensorConditionDto>> scenarioMap = Map.of(scenario, dtos);

        actionConditionPersister.save(scenarioMap);
    }

    private void handleScenarioAddedEvent(ScenarioRemovedEventAvro scenarioRemovedEvent, String hubId) {
        actionConditionPersister.remove(hubId, scenarioRemovedEvent.getName());
    }
}
