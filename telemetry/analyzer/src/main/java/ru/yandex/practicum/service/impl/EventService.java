package ru.yandex.practicum.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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

@Slf4j
@Service
@RequiredArgsConstructor
public class EventService implements HubService<HubEventAvro> {

    private final SensorPersister sensorPersister;
    private final ActionConditionPersister actionConditionPersister;


    @Override
    public void process(HubEventAvro event) {
        log.info("Обработка события для хаба: {}", event.getHubId());
        String hubId = event.getHubId();
        Object payload = event.getPayload();
        log.debug("Получен payload типа: {}", payload.getClass().getSimpleName());

        if (payload instanceof DeviceAddedEventAvro deviceAddedEventAvro) {
            log.info("Обработка DeviceAddedEvent для сенсора: {}", deviceAddedEventAvro.getId());
            handleDeviceAddedEvent(deviceAddedEventAvro, hubId);
            return;
        }
        if (payload instanceof DeviceRemovedEventAvro deviceRemovedEventAvro) {
            log.info("Обработка DeviceRemovedEvent для сенсора: {}", deviceRemovedEventAvro.getId());
            handleDeviceRemovedEvent(deviceRemovedEventAvro, hubId);
            return;
        }
        if (payload instanceof ScenarioAddedEventAvro scenarioAddedEvent) {
            log.info("Обработка ScenarioAddedEvent с именем сценария: {}", scenarioAddedEvent.getName());
            handleScenarioAddedEvent(scenarioAddedEvent, hubId);
            return;
        }
        if (payload instanceof ScenarioRemovedEventAvro scenarioRemovedEvent) {
            log.info("Обработка ScenarioRemovedEvent с именем сценария: {}", scenarioRemovedEvent.getName());
            handleScenarioAddedEvent(scenarioRemovedEvent, hubId);
        }
    }

    private void handleDeviceAddedEvent(DeviceAddedEventAvro event, String hubId) {
        try {
            Sensor sensor = new Sensor();
            sensor.setHubId(hubId);
            sensor.setId(event.getId());
            log.debug("Сохранение нового сенсора: hubId={}, sensorId={}", hubId, event.getId());
            sensorPersister.save(sensor);
            log.info("Сенсор успешно сохранён: {}", sensor);
        } catch (Exception e) {
            log.error("Ошибка при сохранении нового сенсора с id {}: {}", event.getId(), e.getMessage(), e);
        }
    }

    private void handleDeviceRemovedEvent(DeviceRemovedEventAvro event, String hubId) {
        try {
            log.debug("Удаление сенсора: hubId={}, sensorId={}", hubId, event.getId());
            sensorPersister.removeById(event.getId());
            log.info("Сенсор успешно удалён: {}", event.getId());
        } catch (Exception e) {
            log.error("Ошибка при удалении сенсора с id {}: {}", event.getId(), e.getMessage(), e);
        }
    }

    private void handleScenarioAddedEvent(ScenarioAddedEventAvro event, String hubId) {
        try {
            log.info("Начало обработки ScenarioAddedEvent для хаба {} с именем сценария: {}", hubId, event.getName());
            List<DeviceActionAvro> actionsAvro = event.getActions();
            List<ScenarioConditionAvro> conditionsAvro = event.getConditions();

            List<String> sensorsIds = actionsAvro.stream()
                    .map(DeviceActionAvro::getSensorId)
                    .toList();
            log.debug("Получены идентификаторы сенсоров: {}", sensorsIds);
            List<Sensor> sensors = sensorPersister.getSensorsByIds(sensorsIds);
            log.debug("Найдены сенсоры: {}", sensors);

            Map<Long, ActionDto> actionsWithSensorIds = actionsAvro.stream().map(actionAvro -> new ActionDto(
                    Long.parseLong(actionAvro.getSensorId()),
                    actionAvro.getType().name(),
                    actionAvro.getValue()
            )).collect(Collectors.toMap(ActionDto::getSensorId, Function.identity()));
            Map<Long, ConditionDto> conditionsWithSensorIds = conditionsAvro.stream()
                    .map(conditionAvro -> {
                        int value;
                        if (conditionAvro.getValue() instanceof Boolean) {
                            boolean isTrue = (Boolean) conditionAvro.getValue();
                            value = isTrue ? 1 : 0;
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
            log.debug("Сформирован сценарий: {} с условиями: {}", scenario, dtos);
            actionConditionPersister.save(scenarioMap);
            log.info("Сценарий успешно сохранён для хаба {}", hubId);
        } catch (Exception e) {
            log.error("Ошибка при обработке ScenarioAddedEvent: {}", e.getMessage(), e);
        }
    }

    private void handleScenarioAddedEvent(ScenarioRemovedEventAvro scenarioRemovedEvent, String hubId) {
        try {
            log.info("Обработка ScenarioRemovedEvent для хаба {} с именем сценария: {}", hubId, scenarioRemovedEvent.getName());
            actionConditionPersister.remove(hubId, scenarioRemovedEvent.getName());
            log.info("Сценарий с именем {} успешно удалён для хаба {}", scenarioRemovedEvent.getName(), hubId);
        } catch (Exception e) {
            log.error("Ошибка при удалении сценария с именем {}: {}", scenarioRemovedEvent.getName(), e.getMessage(), e);
        }
    }
}
