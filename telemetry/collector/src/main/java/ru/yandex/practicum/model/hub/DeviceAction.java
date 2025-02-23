package ru.yandex.practicum.model.hub;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class DeviceAction {
    // Идентификатор датчика, к которому применяется действие
    private String sensorId;
    // Тип действия: ACTIVATE, DEACTIVATE, INVERSE, SET_VALUE
    private ActionType type;
    // Необязательное значение для действия (например, при SET_VALUE)
    private Integer value;
}
