package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
import ru.yandex.practicum.model.hub.DeviceAction;
import ru.yandex.practicum.model.hub.DeviceAddedEvent;
import ru.yandex.practicum.model.hub.DeviceRemovedEvent;
import ru.yandex.practicum.model.hub.ScenarioAddedEvent;
import ru.yandex.practicum.model.hub.ScenarioCondition;
import ru.yandex.practicum.model.hub.ScenarioRemovedEvent;
import ru.yandex.practicum.model.sensors.ClimateSensorEvent;
import ru.yandex.practicum.model.sensors.LightSensorEvent;
import ru.yandex.practicum.model.sensors.MotionSensorEvent;
import ru.yandex.practicum.model.sensors.SensorEvent;
import ru.yandex.practicum.model.hub.HubEvent;
import ru.yandex.practicum.model.sensors.SwitchSensorEvent;
import ru.yandex.practicum.model.sensors.TemperatureSensorEvent;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<String, Object> sensorKafkaTemplate;
    private final KafkaTemplate<String, Object> hubKafkaTemplate;
    @Value("${sensorEventTopic}")
    private String sensorEventTopic;

    @Value("${hubEventTopic}")
    private String hubEventTopic;


    public void sendSensorEvent(SensorEvent sensorEvent) {
        switch (sensorEvent.getType()) {
            case CLIMATE_SENSOR_EVENT: {
                ClimateSensorEvent climateSensorEvent = (ClimateSensorEvent) sensorEvent;
                // Создаем объект ClimateSensorAvro
                ClimateSensorAvro climateSensorAvro = ClimateSensorAvro.newBuilder()
                        .setTemperatureC(climateSensorEvent.getTemperatureC())
                        .setHumidity(climateSensorEvent.getHumidity())
                        .setCo2Level(climateSensorEvent.getCo2Level())
                        .build();
                // Создаем SensorEventAvro и в payload записываем созданный ClimateSensorAvro
                SensorEventAvro sensorEventAvro = SensorEventAvro.newBuilder()
                        .setId(climateSensorEvent.getId())
                        .setHubId(climateSensorEvent.getHubId())
                        .setTimestamp(climateSensorEvent.getTimestamp().toEpochMilli())
                        .setPayload(climateSensorAvro)
                        .build();
                sensorKafkaTemplate.send(sensorEventTopic, sensorEvent.getHubId(), sensorEventAvro);
                break;
            }
            case LIGHT_SENSOR_EVENT: {
                LightSensorEvent lightSensorEvent = (LightSensorEvent) sensorEvent;
                LightSensorAvro lightSensorAvro = LightSensorAvro.newBuilder()
                        .setLinkQuality(lightSensorEvent.getLinkQuality())
                        .setLuminosity(lightSensorEvent.getLuminosity())
                        .build();
                SensorEventAvro sensorEventAvro = SensorEventAvro.newBuilder()
                        .setId(lightSensorEvent.getId())
                        .setHubId(lightSensorEvent.getHubId())
                        .setTimestamp(lightSensorEvent.getTimestamp().toEpochMilli())
                        .setPayload(lightSensorAvro)
                        .build();
                sensorKafkaTemplate.send(sensorEventTopic, sensorEvent.getHubId(), sensorEventAvro);
                break;
            }
            case MOTION_SENSOR_EVENT: {
                MotionSensorEvent motionSensorEvent = (MotionSensorEvent) sensorEvent;
                MotionSensorAvro motionSensorAvro = MotionSensorAvro.newBuilder()
                        .setLinkQuality(motionSensorEvent.getLinkQuality())
                        .setMotion(motionSensorEvent.isMotion())
                        .setVoltage(motionSensorEvent.getVoltage())
                        .build();
                SensorEventAvro sensorEventAvro = SensorEventAvro.newBuilder()
                        .setId(motionSensorEvent.getId())
                        .setHubId(motionSensorEvent.getHubId())
                        .setTimestamp(motionSensorEvent.getTimestamp().toEpochMilli())
                        .setPayload(motionSensorAvro)
                        .build();
                sensorKafkaTemplate.send(sensorEventTopic, sensorEvent.getHubId(), sensorEventAvro);
                break;
            }
            case SWITCH_SENSOR_EVENT: {
                SwitchSensorEvent switchSensorEvent = (SwitchSensorEvent) sensorEvent;
                SwitchSensorAvro switchSensorAvro = SwitchSensorAvro.newBuilder()
                        .setState(switchSensorEvent.isState())
                        .build();
                SensorEventAvro sensorEventAvro = SensorEventAvro.newBuilder()
                        .setId(switchSensorEvent.getId())
                        .setHubId(switchSensorEvent.getHubId())
                        .setTimestamp(switchSensorEvent.getTimestamp().toEpochMilli())
                        .setPayload(switchSensorAvro)
                        .build();
                sensorKafkaTemplate.send(sensorEventTopic, sensorEvent.getHubId(), sensorEventAvro);
                break;
            }
            case TEMPERATURE_SENSOR_EVENT: {
                TemperatureSensorEvent temperatureSensorEvent = (TemperatureSensorEvent) sensorEvent;
                TemperatureSensorAvro temperatureSensorAvro = TemperatureSensorAvro.newBuilder()
                        .setId(temperatureSensorEvent.getId())
                        .setHubId(temperatureSensorEvent.getHubId())
                        .setTimestamp(temperatureSensorEvent.getTimestamp().toEpochMilli())
                        .setTemperatureC(temperatureSensorEvent.getTemperatureC())
                        .setTemperatureF(temperatureSensorEvent.getTemperatureF())
                        .build();
                SensorEventAvro sensorEventAvro = SensorEventAvro.newBuilder()
                        .setId(temperatureSensorEvent.getId())
                        .setHubId(temperatureSensorEvent.getHubId())
                        .setTimestamp(temperatureSensorEvent.getTimestamp().toEpochMilli())
                        .setPayload(temperatureSensorAvro)
                        .build();
                sensorKafkaTemplate.send(sensorEventTopic, sensorEvent.getHubId(), sensorEventAvro);
                break;
            }
            default:
                throw new IllegalArgumentException("Unknown sensor event type: " + sensorEvent.getType());
        }
    }

    public void sendHubEvent(HubEvent hubEvent) {
        switch (hubEvent.getType()) {
            case DEVICE_ADDED: {
                DeviceAddedEvent deviceAddedEvent = (DeviceAddedEvent) hubEvent;
                DeviceAddedEventAvro deviceAddedEventAvro = DeviceAddedEventAvro.newBuilder()
                        .setId(deviceAddedEvent.getId())
                        .setType(DeviceTypeAvro.valueOf(deviceAddedEvent.getDeviceType().name()))
                        .build();
                HubEventAvro hubEventAvro = HubEventAvro.newBuilder()
                        .setHubId(deviceAddedEvent.getHubId())
                        .setTimestamp(deviceAddedEvent.getTimestamp())
                        .setPayload(deviceAddedEventAvro)
                        .build();
                hubKafkaTemplate.send(hubEventTopic, deviceAddedEvent.getHubId(), hubEventAvro);
                break;
            }
            case DEVICE_REMOVED: {
                DeviceRemovedEvent deviceRemovedEvent = (DeviceRemovedEvent) hubEvent;
                DeviceRemovedEventAvro deviceRemovedEventAvro = DeviceRemovedEventAvro.newBuilder()
                        .setId(deviceRemovedEvent.getId())
                        .build();
                HubEventAvro hubEventAvro = HubEventAvro.newBuilder()
                        .setHubId(deviceRemovedEvent.getHubId())
                        .setTimestamp(deviceRemovedEvent.getTimestamp())
                        .setPayload(deviceRemovedEventAvro)
                        .build();
                hubKafkaTemplate.send(hubEventTopic, deviceRemovedEvent.getHubId(), hubEventAvro);
                break;
            }
            case SCENARIO_ADDED: {
                ScenarioAddedEvent scenarioAddedEvent = (ScenarioAddedEvent) hubEvent;
                log.info("Получено событие SCENARIO_ADDED: {}", scenarioAddedEvent);
                List<ScenarioConditionAvro> avroConditions = scenarioAddedEvent.getConditions().stream()
                        .map(this::mapToAvroScenarioCondition)
                        .toList();
                List<DeviceActionAvro> avroActions = scenarioAddedEvent.getActions().stream()
                        .map(this::mapToAvroDeviceAction)
                        .toList();
                ScenarioAddedEventAvro scenarioAddedEventAvro = ScenarioAddedEventAvro.newBuilder()
                        .setName(scenarioAddedEvent.getName())
                        .setConditions(avroConditions)
                        .setActions(avroActions)
                        .build();
                HubEventAvro hubEventAvro = HubEventAvro.newBuilder()
                        .setHubId(scenarioAddedEvent.getHubId())
                        .setTimestamp(scenarioAddedEvent.getTimestamp())
                        .setPayload(scenarioAddedEventAvro)
                        .build();
                log.info("Отправляю HubEventAvro: {}", hubEventAvro);
                CompletableFuture<SendResult<String, Object>> future =
                        hubKafkaTemplate.send(hubEventTopic, scenarioAddedEvent.getHubId(), hubEventAvro);
                future.whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Ошибка при отправке HubEventAvro для hub {}: ", scenarioAddedEvent.getHubId(), ex);
                    } else {
                        log.info("Сообщение HubEventAvro успешно отправлено. Метаданные: {}", result.getRecordMetadata());
                    }
                });
                break;
            }
            case SCENARIO_REMOVED: {
                ScenarioRemovedEvent scenarioRemovedEvent = (ScenarioRemovedEvent) hubEvent;
                ScenarioRemovedEventAvro scenarioRemovedEventAvro = ScenarioRemovedEventAvro.newBuilder()
                        .setName(scenarioRemovedEvent.getName())
                        .build();
                HubEventAvro hubEventAvro = HubEventAvro.newBuilder()
                        .setHubId(scenarioRemovedEvent.getHubId())
                        .setTimestamp(scenarioRemovedEvent.getTimestamp())
                        .setPayload(scenarioRemovedEventAvro)
                        .build();
                hubKafkaTemplate.send(hubEventTopic, scenarioRemovedEvent.getHubId(), hubEventAvro);
                break;
            }
            default:
                throw new IllegalArgumentException("Unknown hub event type: " + hubEvent.getType());
        }
    }

    private ScenarioConditionAvro mapToAvroScenarioCondition(ScenarioCondition scenarioCondition) {
        return ScenarioConditionAvro.newBuilder()
                .setSensorId(scenarioCondition.getSensorId())
                .setType(ConditionTypeAvro.valueOf(scenarioCondition.getType().name()))
                .setOperation(ConditionOperationAvro.valueOf(scenarioCondition.getOperation().name()))
                .setValue(scenarioCondition.getValue())
                .build();
    }

    private DeviceActionAvro mapToAvroDeviceAction(DeviceAction deviceAction) {
        return DeviceActionAvro.newBuilder()
                .setSensorId(deviceAction.getSensorId())
                .setType(ActionTypeAvro.valueOf(deviceAction.getType().name()))
                .setValue(deviceAction.getValue())
                .build();
    }
}