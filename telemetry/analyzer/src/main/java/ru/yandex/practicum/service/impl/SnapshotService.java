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
        log.info("Начата обработка снапшота для хаба: {}", event.getHubId());
        // Получаем сенсоры по хабу
        List<Sensor> sensors = sensorPersister.getSensorsByHubId(event.getHubId());
        log.debug("Найдено {} сенсоров для хаба {}", sensors.size(), event.getHubId());

        // Определяем, какой сенсор из состояния снапшота обработать
        String processedSensorId = getProcessedSensorId(sensors, event.getSensorsState());
        log.debug("Обрабатываем сенсор с идентификатором: {}", processedSensorId);

        // Извлекаем состояние сенсора (первый найденный)
        SensorStateAvro sensorStateAvro = event.getSensorsState().values().stream().findFirst()
                .orElseThrow(() -> new RuntimeException("Нет состояния сенсора в снапшоте"));
        log.debug("Получено состояние сенсора: {}", sensorStateAvro);

        // Получаем сценарии для сенсора
        List<ScenarioAction> scenarioActions = scenarioActionPersister.getScenarioActionsBySensorId(processedSensorId);
        log.debug("Найдено {} сценарных действий для сенсора {}", scenarioActions.size(), processedSensorId);

        Map<Long, ScenarioCondition> scenarioConditions = scenarioConditionPersister.getScenarioConditionsBySensorId(processedSensorId).stream()
                .collect(Collectors.toMap(it -> it.getScenario().getId(), Function.identity()));
        log.debug("Найдено {} условий сценариев для сенсора {}", scenarioConditions.size(), processedSensorId);

        // Построение DTO для сценарных действий
        List<ScenarioActionConditionDto> dtos = scenarioActions.stream()
                .map(scenarioAction -> {
                    ScenarioActionConditionDto dto = new ScenarioActionConditionDto();
                    dto.setAction(scenarioAction.getAction());
                    dto.setSensor(scenarioAction.getSensor());
                    dto.setScenario(scenarioAction.getScenario());
                    dto.setCondition(scenarioConditions.get(scenarioAction.getScenario().getId()).getCondition());
                    return dto;
                })
                .toList();
        log.info("DTO для сценариев сформированы. Количество DTO: {}", dtos.size());

        // Определяем тип данных и вызываем соответствующий builder
        Object data = sensorStateAvro.getData();
        switch (data) {
            case ClimateSensorAvro climateSensorAvro -> {
                log.info("Обработка данных климатического сенсора");
                List<DeviceActionRequest> requests = climateSensorDeviceActionBuilder.build(climateSensorAvro, dtos);
                log.debug("Сформировано {} команд для климатического сенсора", requests.size());
                handleDeviceAction(requests);
            }
            case LightSensorAvro lightSensorAvro -> {
                log.info("Обработка данных сенсора освещенности");
                List<DeviceActionRequest> requests = lightSensorDeviceActionBuilder.build(lightSensorAvro, dtos);
                log.debug("Сформировано {} команд для сенсора освещенности", requests.size());
                handleDeviceAction(requests);
            }
            case MotionSensorAvro motionSensorAvro -> {
                log.info("Обработка данных сенсора движения");
                List<DeviceActionRequest> requests = motionSensorDeviceActionBuilder.build(motionSensorAvro, dtos);
                log.debug("Сформировано {} команд для сенсора движения", requests.size());
                handleDeviceAction(requests);
            }
            case SwitchSensorAvro switchSensorAvro -> {
                log.info("Обработка данных сенсора переключателя");
                List<DeviceActionRequest> requests = switchSensorDeviceActionBuilder.build(switchSensorAvro, dtos);
                log.debug("Сформировано {} команд для сенсора переключателя", requests.size());
                handleDeviceAction(requests);
            }
            case TemperatureSensorAvro temperatureSensorAvro -> {
                log.info("Обработка данных температурного сенсора");
                List<DeviceActionRequest> requests = temperatureSensorDeviceActionBuilder.build(temperatureSensorAvro, dtos);
                log.debug("Сформировано {} команд для температурного сенсора", requests.size());
                handleDeviceAction(requests);
            }
            case null, default -> log.warn("Тип данных сенсора не определён: {}", data.getClass().getName());
        }
    }

    private String getProcessedSensorId(List<Sensor> sensors, Map<String, SensorStateAvro> sensorsState) {
        AtomicReference<String> sensorId = new AtomicReference<>();
        try {
            sensors.forEach(it -> {
                if (sensorsState.containsKey(it.getId())) {
                    sensorId.set(it.getId());
                    log.debug("Сенсор {} найден в состоянии снапшота", it.getId());
                } else {
                    log.warn("Сенсор {} отсутствует в состоянии снапшота", it.getId());
                }
            });
        } catch (Exception e) {
            log.error("Не найден сенсор для обработки снапшота", e);
        }
        return sensorId.get();
    }

    private void handleDeviceAction(List<DeviceActionRequest> requests) {
        if (requests.isEmpty()) {
            log.debug("Нет сформированных команд для отправки в Hub Router");
            return;
        }
        requests.forEach(request -> {
            log.info("Отправка команды в Hub Router: {}", request);
            try {
                var response = hubRouter.handleDeviceAction(request);
                log.debug("Команда успешно отправлена: {}. Ответ: {}", request, response);
            } catch (Exception ex) {
                log.error("Ошибка при отправке команды: {}. Ошибка: {}", request, ex.getMessage(), ex);
            }
        });
    }
}
