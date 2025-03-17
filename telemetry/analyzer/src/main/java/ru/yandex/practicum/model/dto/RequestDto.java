package ru.yandex.practicum.model.dto;

import lombok.Builder;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;

@Builder
public record RequestDto(int value, String sensorId, ActionTypeProto type) {

}
