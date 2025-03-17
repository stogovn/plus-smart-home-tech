package ru.yandex.practicum.service.impl;

import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.model.Action;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.Sensor;
import ru.yandex.practicum.model.dto.ScenarioActionConditionDto;
import ru.yandex.practicum.service.DeviceActionBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro.MOTION;

@Service
public class MotionSensorDeviceActionBuilder implements DeviceActionBuilder<MotionSensorAvro> {

    private final Predicate<ScenarioActionConditionDto> filterByType = scenarioCondition -> {
        String type = scenarioCondition.getCondition().getType();
        return type.equals(MOTION.name());
    };

    @Override
    public List<DeviceActionRequest> build(MotionSensorAvro data, List<ScenarioActionConditionDto> dtos) {
        List<DeviceActionRequest> requests = new ArrayList<>();
        dtos.stream()
                .filter(filterByType)
                .forEach(scenarioActionConditionDto -> {
                    Sensor sensor = scenarioActionConditionDto.getSensor();
                    Condition condition = scenarioActionConditionDto.getCondition();
                    Action action = scenarioActionConditionDto.getAction();
                    int motion = data.getMotion() ? 1 : 0;
                    switch (condition.getOperation()) {
                        case "БОЛЬШЕ" -> {
                            DeviceActionRequest request = buildRequestDto(sensor.getId(), action.getType(),
                                    action.getValue());
                            if (motion > condition.getValue()) {
                                requests.add(request);
                            }
                        }
                        case "МЕНЬШЕ" -> {
                            DeviceActionRequest request = buildRequestDto(sensor.getId(), action.getType(),
                                    action.getValue());
                            if (motion < condition.getValue()) {
                                requests.add(request);
                            }
                        }
                        case "РАВНО" -> {
                            DeviceActionRequest request = buildRequestDto(sensor.getId(), action.getType(),
                                    action.getValue());
                            if (motion == condition.getValue()) {
                                requests.add(request);
                            }
                        }
                    }
                });
        return requests;
    }
}
