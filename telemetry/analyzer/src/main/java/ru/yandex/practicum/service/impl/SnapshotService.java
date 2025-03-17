package ru.yandex.practicum.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
import ru.yandex.practicum.model.ScenarioAction;
import ru.yandex.practicum.model.ScenarioCondition;
import ru.yandex.practicum.model.Sensor;
import ru.yandex.practicum.model.dto.ScenarioActionConditionDto;
import ru.yandex.practicum.repository.persister.ScenarioActionPersister;
import ru.yandex.practicum.repository.persister.ScenarioConditionPersister;
import ru.yandex.practicum.repository.persister.SensorPersister;
import ru.yandex.practicum.service.DeviceActionBuilder;
import ru.yandex.practicum.service.HubService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class SnapshotService implements HubService<SensorsSnapshotAvro> {

    private final SensorPersister sensorPersister;
    private final ScenarioActionPersister scenarioActionPersister;
    private final ScenarioConditionPersister scenarioConditionPersister;
    @Qualifier("lightSensorDeviceActionBuilder")
    private final DeviceActionBuilder<LightSensorAvro> lightSensorDeviceActionBuilder;
    @Qualifier("climateSensorDeviceActionBuilder")
    private final DeviceActionBuilder<ClimateSensorAvro> climateSensorDeviceActionBuilder;
    @Qualifier("motionSensorDeviceActionBuilder")
    private final DeviceActionBuilder<MotionSensorAvro> motionSensorDeviceActionBuilder;
    @Qualifier("switchSensorDeviceActionBuilder")
    private final DeviceActionBuilder<SwitchSensorAvro> switchSensorDeviceActionBuilder;
    @Qualifier("temperatureSensorDeviceActionBuilder")
    private final DeviceActionBuilder<TemperatureSensorAvro> temperatureSensorDeviceActionBuilder;

    @GrpcClient("hub-router")
    private HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouter;


    @Override
    public void process(SensorsSnapshotAvro event) {
        List<Sensor> sensors = sensorPersister.getSensorsByHubId(event.getHubId());
        String processedSensorId = getProcessedSensorId(sensors, event.getSensorsState());
        SensorStateAvro sensorStateAvro = event.getSensorsState().values().stream().findFirst().orElseThrow();
        List<ScenarioAction> scenarioActions = scenarioActionPersister.getScenarioActionsBySensorId(
                processedSensorId);
        Map<Long, ScenarioCondition> scenarioConditions = scenarioConditionPersister.getScenarioConditionsBySensorId(
                        processedSensorId).stream()
                .collect(Collectors.toMap(it -> it.getScenario().getId(), Function.identity()));

        List<ScenarioActionConditionDto> dtos = scenarioActions.stream()
                .map(scenarioAction -> {
                    ScenarioActionConditionDto dto = new ScenarioActionConditionDto();
                    dto.setAction(scenarioAction.getAction());
                    dto.setSensor(scenarioAction.getSensor());
                    dto.setScenario(scenarioAction.getScenario());
                    dto.setCondition(scenarioConditions.get(scenarioAction.getScenario().getId()).getCondition());
                    return dto;
                }).toList();

        Object data = sensorStateAvro.getData();
        if (data instanceof ClimateSensorAvro climateSensorAvro) {
            List<DeviceActionRequest> requests = climateSensorDeviceActionBuilder.build(climateSensorAvro, dtos);
            handleDeviceAction(requests);
        }
        if (data instanceof LightSensorAvro lightSensorAvro) {
            List<DeviceActionRequest> requests = lightSensorDeviceActionBuilder.build(lightSensorAvro, dtos);
            handleDeviceAction(requests);
        }
        if (data instanceof MotionSensorAvro motionSensorAvro) {
            List<DeviceActionRequest> requests = motionSensorDeviceActionBuilder.build(motionSensorAvro, dtos);
            handleDeviceAction(requests);
        }
        if (data instanceof SwitchSensorAvro switchSensorAvro) {
            List<DeviceActionRequest> requests = switchSensorDeviceActionBuilder.build(switchSensorAvro, dtos);
            handleDeviceAction(requests);
        }
        if (data instanceof TemperatureSensorAvro temperatureSensorAvro) {
            List<DeviceActionRequest> requests = temperatureSensorDeviceActionBuilder.build(temperatureSensorAvro,
                    dtos);
            handleDeviceAction(requests);
        }

    }

    private String getProcessedSensorId(List<Sensor> sensors, Map<String, SensorStateAvro> sensorsState) {
        AtomicReference<String> sensorId = new AtomicReference<>();
        try {
            sensors.forEach(it -> {
                if (sensorsState.containsKey(it.getId())) {
                    sensorId.set(it.getId());
                    return;
                }
                throw new RuntimeException("Не могу найти сенсор с идентификатором " + it.getId());
            });
        } catch (Exception e) {
            log.error("не найден сенсор для обработки снапшота", e);
        }
        return sensorId.get();
    }

    private void handleDeviceAction(List<DeviceActionRequest> requests) {
        if (requests.isEmpty()) {
            return;
        }
        requests.forEach(hubRouter::handleDeviceAction);
    }
}
