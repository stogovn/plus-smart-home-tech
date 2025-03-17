package ru.yandex.practicum.model.dto;

import lombok.Data;

@Data
public class ActionDto {

    private final Long sensorId;

    private final String type;

    private final Integer value;

}
