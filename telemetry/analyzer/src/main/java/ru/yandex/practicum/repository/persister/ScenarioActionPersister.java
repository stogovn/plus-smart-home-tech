package ru.yandex.practicum.repository.persister;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.model.ScenarioAction;
import ru.yandex.practicum.repository.ScenarioActionRepository;

import java.util.List;

@Component
@Transactional
@RequiredArgsConstructor
public class ScenarioActionPersister {

    private final ScenarioActionRepository scenarioActionRepository;

    public List<ScenarioAction> getScenarioActionsBySensorId(String sensorId) {
        return scenarioActionRepository.getScenarioActionsBySensor_Id(sensorId);
    }
}
