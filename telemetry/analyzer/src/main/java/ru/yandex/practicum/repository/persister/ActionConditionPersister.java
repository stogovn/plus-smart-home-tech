package ru.yandex.practicum.repository.persister;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.model.Action;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.model.ScenarioAction;
import ru.yandex.practicum.model.ScenarioCondition;
import ru.yandex.practicum.model.dto.ActionSensorConditionDto;
import ru.yandex.practicum.repository.ActionRepository;
import ru.yandex.practicum.repository.ConditionRepository;
import ru.yandex.practicum.repository.ScenarioActionRepository;
import ru.yandex.practicum.repository.ScenarioConditionRepository;
import ru.yandex.practicum.repository.ScenarioRepository;

import java.util.List;
import java.util.Map;

@Component
@Transactional
@RequiredArgsConstructor
public class ActionConditionPersister {

    private final ActionRepository actionRepository;
    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ScenarioActionRepository scenarioActionRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;

    public void save(Map<Scenario, List<ActionSensorConditionDto>> scenarioMap) {
        Scenario scenario = scenarioMap.keySet().iterator().next();

        scenarioRepository.findByHubIdAndName(scenario.getHubId(), scenario.getName())
                .ifPresent(scenario1 -> remove(scenario1.getHubId(), scenario1.getName()));

        List<ActionSensorConditionDto> dtos = scenarioMap.values().stream().findFirst()
                .orElseThrow();
        List<Action> actions = dtos.stream()
                .map(ActionSensorConditionDto::action)
                .toList();
        List<Condition> conditions = dtos.stream()
                .map(ActionSensorConditionDto::condition)
                .toList();
        List<ScenarioAction> scenarioActions = dtos.stream()
                .map(dto -> {
                    ScenarioAction scenarioAction = new ScenarioAction();
                    scenarioAction.setAction(dto.action());
                    scenarioAction.setScenario(scenario);
                    scenarioAction.setSensor(dto.sensor());
                    return scenarioAction;
                }).toList();
        List<ScenarioCondition> scenarioConditions = dtos.stream()
                .map(dto -> {
                    ScenarioCondition scenarioCondition = new ScenarioCondition();
                    scenarioCondition.setSensor(dto.sensor());
                    scenarioCondition.setScenario(scenario);
                    scenarioCondition.setCondition(dto.condition());
                    return scenarioCondition;
                }).toList();

        scenarioRepository.save(scenario);
        actionRepository.saveAll(actions);
        conditionRepository.saveAll(conditions);
        scenarioActionRepository.saveAll(scenarioActions);
        scenarioConditionRepository.saveAll(scenarioConditions);
    }

    public void remove(String hubId, String scenarioName) {
        Scenario scenario = scenarioRepository.findByHubIdAndName(hubId, scenarioName).orElseThrow();
        List<Action> actions = scenario.getScenarioActions().stream()
                .map(ScenarioAction::getAction)
                .toList();
        List<Condition> conditions = scenario.getScenarioConditions().stream()
                .map(ScenarioCondition::getCondition)
                .toList();

        scenarioRepository.delete(scenario);
        actionRepository.deleteAll(actions);
        conditionRepository.deleteAll(conditions);
    }
}
