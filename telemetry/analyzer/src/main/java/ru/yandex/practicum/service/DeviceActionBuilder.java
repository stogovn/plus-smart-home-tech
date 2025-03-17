package ru.yandex.practicum.service;

import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.model.dto.ScenarioActionConditionDto;

import java.util.List;

public interface DeviceActionBuilder<T extends SpecificRecordBase> {

    List<DeviceActionRequest> build(T data, List<ScenarioActionConditionDto> dtos);

    default DeviceActionRequest buildRequestDto(String sensorId, String action, int value) {
        return DeviceActionRequest.newBuilder()
                .setSensorId(sensorId)
                .setType(ActionTypeProto.valueOf(action))
                .setValue(value)
                .build();
    }
}