package ru.yandex.practicum.model.dto;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class ConditionDto {

    private final Long sensorId;

    private final String type;

    private final String operation;

    private final Integer value;
}