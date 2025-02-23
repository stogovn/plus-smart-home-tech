package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
import ru.yandex.practicum.model.hub.DeviceAddedEvent;
import ru.yandex.practicum.model.hub.DeviceRemovedEvent;
import ru.yandex.practicum.model.hub.ScenarioAddedEvent;
import ru.yandex.practicum.model.hub.ScenarioRemovedEvent;
import ru.yandex.practicum.model.sensors.ClimateSensorEvent;
import ru.yandex.practicum.model.sensors.LightSensorEvent;
import ru.yandex.practicum.model.sensors.MotionSensorEvent;
import ru.yandex.practicum.model.sensors.SensorEvent;
import ru.yandex.practicum.model.hub.HubEvent;
import ru.yandex.practicum.model.sensors.SwitchSensorEvent;
import ru.yandex.practicum.model.sensors.TemperatureSensorEvent;

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
                ClimateSensorAvro climateSensorAvro = ClimateSensorAvro.newBuilder()
                        .setTemperatureC(climateSensorEvent.getTemperatureC())
                        .setHumidity(climateSensorEvent.getHumidity())
                        .setCo2Level(climateSensorEvent.getCo2Level())
                        .build();
                sensorKafkaTemplate.send(sensorEventTopic, sensorEvent.getHubId(), climateSensorAvro);
                break;
            }
            case LIGHT_SENSOR_EVENT: {
                LightSensorEvent lightSensorEvent = (LightSensorEvent) sensorEvent;
                LightSensorAvro lightSensorAvro = LightSensorAvro.newBuilder()
                        .setLinkQuality(lightSensorEvent.getLinkQuality())
                        .setLuminosity(lightSensorEvent.getLuminosity())
                        .build();
                sensorKafkaTemplate.send(sensorEventTopic, sensorEvent.getHubId(), lightSensorAvro);
                break;
            }
            case MOTION_SENSOR_EVENT: {
                MotionSensorEvent motionSensorEvent = (MotionSensorEvent) sensorEvent;
                MotionSensorAvro motionSensorAvro = MotionSensorAvro.newBuilder()
                        .setLinkQuality(motionSensorEvent.getLinkQuality())
                        .setMotion(motionSensorEvent.isMotion())
                        .setVoltage(motionSensorEvent.getVoltage())
                        .build();
                sensorKafkaTemplate.send(sensorEventTopic, sensorEvent.getHubId(), motionSensorAvro);
                break;
            }
            case SWITCH_SENSOR_EVENT: {
                SwitchSensorEvent switchSensorEvent = (SwitchSensorEvent) sensorEvent;
                SwitchSensorAvro switchSensorAvro = SwitchSensorAvro.newBuilder()
                        .setState(switchSensorEvent.isState())
                        .build();
                sensorKafkaTemplate.send(sensorEventTopic, sensorEvent.getHubId(), switchSensorAvro);
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
                sensorKafkaTemplate.send(sensorEventTopic, sensorEvent.getHubId(), temperatureSensorAvro);
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
                        .setType(deviceAddedEvent.getDeviceType()) // предполагается, что тип совместим или требует конвертации
                        .build();
                hubKafkaTemplate.send(hubEventTopic, hubEvent.getHubId(), deviceAddedEventAvro);
                break;
            }
            case DEVICE_REMOVED: {
                DeviceRemovedEvent deviceRemovedEvent = (DeviceRemovedEvent) hubEvent;
                DeviceRemovedEventAvro deviceRemovedEventAvro = DeviceRemovedEventAvro.newBuilder()
                        .setId(deviceRemovedEvent.getId())
                        .build();
                hubKafkaTemplate.send(hubEventTopic, hubEvent.getHubId(), deviceRemovedEventAvro);
                break;
            }
            case SCENARIO_ADDED: {
                ScenarioAddedEvent scenarioAddedEvent = (ScenarioAddedEvent) hubEvent;
                ScenarioAddedEventAvro scenarioAddedEventAvro = ScenarioAddedEventAvro.newBuilder()
                        .setName(scenarioAddedEvent.getName())
                        .setActions(scenarioAddedEvent.getActions())
                        .setConditions(scenarioAddedEvent.getConditions())
                        .build();
                hubKafkaTemplate.send(hubEventTopic, hubEvent.getHubId(), scenarioAddedEventAvro);
                break;
            }
            case SCENARIO_REMOVED: {
                ScenarioRemovedEvent scenarioRemovedEvent = (ScenarioRemovedEvent) hubEvent;
                ScenarioRemovedEventAvro scenarioRemovedEventAvro = ScenarioRemovedEventAvro.newBuilder()
                        .setName(scenarioRemovedEvent.getName())
                        .build();
                hubKafkaTemplate.send(hubEventTopic, hubEvent.getHubId(), scenarioRemovedEventAvro);
                break;
            }
            default:
                throw new IllegalArgumentException("Unknown hub event type: " + hubEvent.getType());
        }
    }
}