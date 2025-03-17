package ru.yandex.practicum.model.dto;

import ru.yandex.practicum.model.Action;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.Sensor;

public record ActionSensorConditionDto(Condition condition,
                                       Sensor sensor,
                                       Action action) {

}
