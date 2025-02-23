package ru.yandex.practicum.model.hub;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class ScenarioCondition {
    // Идентификатор датчика, по которому проверяется условие
    private String sensorId;
    // Тип условия: MOTION, LUMINOSITY, SWITCH, TEMPERATURE, CO2LEVEL, HUMIDITY
    private ConditionType type;
    // Операция условия: EQUALS, GREATER_THAN, LOWER_THAN
    private ConditionOperation operation;
    // Значение для сравнения (может быть null)
    private Integer value;
}
