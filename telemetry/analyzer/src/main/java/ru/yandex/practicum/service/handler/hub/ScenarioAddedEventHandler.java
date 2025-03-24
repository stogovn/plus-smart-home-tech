package ru.yandex.practicum.service.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.exception.NotFoundException;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.model.Action;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.repository.ActionRepository;
import ru.yandex.practicum.repository.ConditionRepository;
import ru.yandex.practicum.repository.ScenarioRepository;
import ru.yandex.practicum.repository.SensorRepository;
import ru.yandex.practicum.service.handler.Mapper;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static ru.yandex.practicum.service.handler.Mapper.mapToAction;
import static ru.yandex.practicum.service.handler.Mapper.mapToCondition;

@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioAddedEventHandler implements HubEventHandler {
    private final ScenarioRepository scenarioRepository;
    private final SensorRepository sensorRepository;
    private final ActionRepository actionRepository;
    private final ConditionRepository conditionRepository;

    @Override
    public String getEventType() {
        return ScenarioAddedEventAvro.class.getName();
    }

    @Transactional
    @Override
    public void handle(HubEventAvro hubEvent) {
        log.info("==> Handle hubEvent = {}", hubEvent);
        ScenarioAddedEventAvro scenarioEvent = (ScenarioAddedEventAvro) hubEvent.getPayload();
        validateSensors(scenarioEvent.getConditions(), scenarioEvent.getActions(), hubEvent.getHubId());

        Optional<Scenario> existingScenario = scenarioRepository.findByHubIdAndName(hubEvent.getHubId(), scenarioEvent.getName());
        Scenario scenario;
        String logAction;
        List<Long> oldConditionIds = null;
        List<Long> oldActionIds = null;

        if (existingScenario.isEmpty()) {
            scenario = Mapper.mapToScenario(hubEvent, scenarioEvent);
            logAction = "Added";
        } else {
            scenario = existingScenario.get();
            oldConditionIds = scenario.getConditions().values().stream().map(Condition::getId).collect(Collectors.toList());
            oldActionIds = scenario.getActions().values().stream().map(Action::getId).collect(Collectors.toList());

            scenario.setConditions(new HashMap<>());
            for (ScenarioConditionAvro condition : scenarioEvent.getConditions()) {
                scenario.getConditions().put(condition.getSensorId(), mapToCondition(condition));
            }

            scenario.setActions(new HashMap<>());
            for (DeviceActionAvro action : scenarioEvent.getActions()) {
                scenario.getActions().put(action.getSensorId(), mapToAction(action));
            }
            logAction = "Updated";
        }

        scenarioRepository.save(scenario);
        log.info("== {} scenario = {}", logAction, scenario);
        cleanupUnusedConditions(oldConditionIds);
        cleanupUnusedActions(oldActionIds);
        log.info("<== After cleanup");
    }

    private void validateSensors(Collection<ScenarioConditionAvro> conditions, Collection<DeviceActionAvro> actions,
                                 String hubId) {
        List<String> conditionSensorIds = getConditionSensorIds(conditions);
        List<String> actionSensorIds = getActionSensorIds(actions);

        if (!sensorRepository.existsByIdInAndHubId(conditionSensorIds, hubId)) {
            throw new NotFoundException("Sensors for conditions of the scenario not found.");
        }
        if (!sensorRepository.existsByIdInAndHubId(actionSensorIds, hubId)) {
            throw new NotFoundException("Sensors for actions of the scenario not found.");
        }
    }

    private List<String> getConditionSensorIds(Collection<ScenarioConditionAvro> conditions) {
        return conditions.stream().map(ScenarioConditionAvro::getSensorId).toList();
    }

    private List<String> getActionSensorIds(Collection<DeviceActionAvro> actions) {
        return actions.stream().map(DeviceActionAvro::getSensorId).toList();
    }

    private void cleanupUnusedConditions(Collection<Long> conditionIds) {
        if (conditionIds != null && !conditionIds.isEmpty()) {
            conditionRepository.deleteAllById(conditionIds);
        }
    }

    private void cleanupUnusedActions(Collection<Long> actionIds) {
        if (actionIds != null && !actionIds.isEmpty()) {
            actionRepository.deleteAllById(actionIds);
        }
    }
}
