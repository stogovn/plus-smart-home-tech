package ru.yandex.practicum.handler.hub;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
import ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;

import java.time.Instant;
import java.util.List;

@Component
public class ScenarioAddedEventHandler extends AbstractHubEventHandler {
    public ScenarioAddedEventHandler(KafkaTemplate<String, Object> hubKafkaTemplate) {
        super(hubKafkaTemplate);
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_ADDED;
    }

    @Override
    protected HubEventAvro mapToAvro(HubEventProto eventProto) {
        ScenarioAddedEventProto scenarioAddedEventProto = eventProto.getScenarioAdded();
        List<ScenarioConditionAvro> scenarioConditionAvroList = scenarioAddedEventProto.getConditionList()
                .stream().map(this::mapToAvroScenarioCondition).toList();
        List<DeviceActionAvro> deviceActionAvroList = scenarioAddedEventProto.getActionList()
                .stream().map(this::mapToAvroDeviceAction).toList();
        ScenarioAddedEventAvro scenarioAddedEventAvro = ScenarioAddedEventAvro.newBuilder()
                .setName(scenarioAddedEventProto.getName())
                .setConditions(scenarioConditionAvroList)
                .setActions(deviceActionAvroList)
                .build();
        return HubEventAvro.newBuilder()
                .setHubId(eventProto.getHubId())
                .setTimestamp(Instant.ofEpochSecond(eventProto.getTimestamp().getSeconds(),
                        eventProto.getTimestamp().getNanos()))
                .setPayload(scenarioAddedEventAvro)
                .build();
    }

    private ScenarioConditionAvro mapToAvroScenarioCondition(ScenarioConditionProto scenarioConditionProto) {
        Object value = null;
        if (scenarioConditionProto.getValueCase().equals(ScenarioConditionProto.ValueCase.INT_VALUE)) {
            value = scenarioConditionProto.getIntValue();
        } else if (scenarioConditionProto.getValueCase().equals(ScenarioConditionProto.ValueCase.BOOL_VALUE)) {
            value = scenarioConditionProto.getBoolValue();
        }
        return ScenarioConditionAvro.newBuilder()
                .setSensorId(scenarioConditionProto.getSensorId())
                .setType(ConditionTypeAvro.valueOf(scenarioConditionProto.getType().name()))
                .setOperation(ConditionOperationAvro.valueOf(scenarioConditionProto.getOperation().name()))
                .setValue(value)
                .build();
    }

    private DeviceActionAvro mapToAvroDeviceAction(DeviceActionProto deviceActionProto) {
        return DeviceActionAvro.newBuilder()
                .setSensorId(deviceActionProto.getSensorId())
                .setType(DeviceActionTypeAvro.valueOf(deviceActionProto.getType().name()))
                .setValue(deviceActionProto.getValue())
                .build();
    }
}
