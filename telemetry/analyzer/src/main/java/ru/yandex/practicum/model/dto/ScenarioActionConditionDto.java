package ru.yandex.practicum.model.dto;

import lombok.Getter;
import lombok.Setter;
import ru.yandex.practicum.model.Action;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.model.Sensor;

@Getter
@Setter
public class ScenarioActionConditionDto {

    private Sensor sensor;
    private Action action;
    private Scenario scenario;
    private Condition condition;
}
