package ru.yandex.practicum.repository.persister;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.model.ScenarioCondition;
import ru.yandex.practicum.repository.ScenarioConditionRepository;

import java.util.List;

@Component
@Transactional
@RequiredArgsConstructor
public class ScenarioConditionPersister {

    private final ScenarioConditionRepository scenarioConditionRepository;

    @Transactional(readOnly = true)
    public List<ScenarioCondition> getScenarioConditionsBySensorId(String sensorId) {
        return scenarioConditionRepository.getScenarioConditionsBySensor_Id(sensorId);
    }
}
