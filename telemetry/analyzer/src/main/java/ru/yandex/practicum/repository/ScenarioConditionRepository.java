package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.model.ScenarioCondition;
import ru.yandex.practicum.model.ScenarioConditionId;
import ru.yandex.practicum.model.Sensor;

import java.util.Collection;
import java.util.List;

@Repository
public interface ScenarioConditionRepository extends JpaRepository<ScenarioCondition, ScenarioConditionId> {


    List<ScenarioCondition> getScenarioConditionsBySensorIn(Collection<Sensor> sensor);

    List<ScenarioCondition> getScenarioConditionsBySensor_Id(String id);
}
