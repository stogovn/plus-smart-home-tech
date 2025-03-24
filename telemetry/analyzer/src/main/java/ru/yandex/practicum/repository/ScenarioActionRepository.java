package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.model.ScenarioAction;
import ru.yandex.practicum.model.ScenarioActionCompositeKey;

@Repository
public interface ScenarioActionRepository extends JpaRepository<ScenarioAction, ScenarioActionCompositeKey> {
}
