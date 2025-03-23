package ru.yandex.practicum.service.handler;

import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.model.Action;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.model.Sensor;

import java.util.HashMap;

public class Mapper {

    public static Sensor mapToSensor(HubEventAvro hubEventAvro, DeviceAddedEventAvro deviceAddedEventAvro) {
        return new Sensor(
                deviceAddedEventAvro.getId(),
                hubEventAvro.getHubId()
        );
    }

    public static Scenario mapToScenario(HubEventAvro hubEventAvro, ScenarioAddedEventAvro scenarioAddedEventAvro) {
        Scenario scenario = new Scenario();
        scenario.setHubId(hubEventAvro.getHubId());
        scenario.setName(scenarioAddedEventAvro.getName());

        scenario.setConditions(new HashMap<>());
        for (ScenarioConditionAvro condition : scenarioAddedEventAvro.getConditions()) {
            scenario.getConditions().put(condition.getSensorId(), mapToCondition(condition));
        }

        scenario.setActions(new HashMap<>());
        for (DeviceActionAvro action : scenarioAddedEventAvro.getActions()) {
            scenario.getActions().put(action.getSensorId(), mapToAction(action));
        }

        return scenario;
    }

    public static Condition mapToCondition(ScenarioConditionAvro conditionAvro) {
        return Condition.builder()
                .type(conditionAvro.getType())
                .operation(conditionAvro.getOperation())
                .value(getConditionValue(conditionAvro.getValue()))
                .build();
    }

    public static Action mapToAction(DeviceActionAvro deviceActionAvro) {
        return Action.builder()
                .type(deviceActionAvro.getType())
                .value(deviceActionAvro.getValue())
                .build();
    }

    public static Integer getConditionValue(Object conditionValue) {
        if (conditionValue == null) {
            return null;
        }
        if (conditionValue instanceof Boolean) {
            return ((Boolean) conditionValue ? 1 : 0);
        }
        if (conditionValue instanceof Integer) {
            return (Integer) conditionValue;
        }
        throw new ClassCastException("Error while converting value");
    }
}
