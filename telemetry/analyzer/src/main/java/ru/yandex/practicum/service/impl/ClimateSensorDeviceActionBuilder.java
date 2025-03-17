package ru.yandex.practicum.service.impl;

import static ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro.CO2LEVEL;
import static ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro.HUMIDITY;

import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.model.Action;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.Sensor;
import ru.yandex.practicum.model.dto.ScenarioActionConditionDto;
import ru.yandex.practicum.service.DeviceActionBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

@Service
public class ClimateSensorDeviceActionBuilder implements DeviceActionBuilder<ClimateSensorAvro> {

    private final Predicate<ScenarioActionConditionDto> filterByType = scenarioCondition -> {
        String type = scenarioCondition.getCondition().getType();
        return type.equals(HUMIDITY.name()) || type.equals(CO2LEVEL.name());
    };

    @Override
    public List<DeviceActionRequest> build(ClimateSensorAvro data, List<ScenarioActionConditionDto> dtos) {
        List<DeviceActionRequest> requests = new ArrayList<>();
        dtos.stream()
                .filter(filterByType)
                .forEach(scenarioActionConditionDto -> {
                    Sensor sensor = scenarioActionConditionDto.getSensor();
                    Condition condition = scenarioActionConditionDto.getCondition();
                    Action action = scenarioActionConditionDto.getAction();

                    switch (condition.getOperation()) {
                        case "БОЛЬШЕ" -> {
                            DeviceActionRequest request = buildRequestDto(sensor.getId(), action.getType(),
                                    action.getValue());
                            if (data.getHumidity() > condition.getValue() && condition.getType()
                                    .equals(HUMIDITY.name())) {
                                requests.add(request);
                            }
                            if (data.getCo2Level() > condition.getValue() && condition.getType()
                                    .equals(CO2LEVEL.name())) {
                                requests.add(request);
                            }
                        }
                        case "МЕНЬШЕ" -> {
                            DeviceActionRequest request = buildRequestDto(sensor.getId(), action.getType(),
                                    action.getValue());
                            if (data.getHumidity() < condition.getValue() && condition.getType()
                                    .equals(HUMIDITY.name())) {
                                requests.add(request);
                            }
                            if (data.getCo2Level() < condition.getValue() && condition.getType()
                                    .equals(CO2LEVEL.name())) {
                                requests.add(request);
                            }
                        }
                        case "РАВНО" -> {
                            DeviceActionRequest request = buildRequestDto(sensor.getId(), action.getType(),
                                    action.getValue());
                            if (data.getHumidity() == condition.getValue() && condition.getType()
                                    .equals(HUMIDITY.name())) {
                                requests.add(request);
                            }
                            if (data.getCo2Level() == condition.getValue() && condition.getType()
                                    .equals(CO2LEVEL.name())) {
                                requests.add(request);
                            }
                        }
                    }
                });
        return requests;
    }
}
