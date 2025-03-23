package ru.yandex.practicum.service;

import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.model.Action;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.service.handler.hub.HubEventHandler;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
public class HubEventServiceImpl implements HubEventService {

    private final ScenarioExecutor scenarioExecutor;
    private final Map<String, HubEventHandler> hubEventHandlers;

    public HubEventServiceImpl(Set<HubEventHandler> hubEventHandlers, ScenarioExecutor scenarioExecutor) {
        this.hubEventHandlers = hubEventHandlers.stream()
                .collect(Collectors.toMap(
                        HubEventHandler::getEventType,
                        Function.identity()
                ));
        this.scenarioExecutor = scenarioExecutor;
    }

    @Override
    public void process(HubEventAvro hubEvent) {
        log.info("==> Process hubEvent = {}", hubEvent);
        String eventType = hubEvent.getPayload().getClass().getName();
        if (hubEventHandlers.containsKey(eventType)) {
            log.info("== Handle event by handler: {}", hubEventHandlers.get(eventType));
            hubEventHandlers.get(eventType).handle(hubEvent);
        } else {
            throw new IllegalArgumentException("Handler not found for sensor eventType =  " + eventType);
        }
    }

    @Override
    public void sendActionsByScenario(Scenario scenario) {
        log.info("==> Send actions by scenario = {}", scenario);
        String hubId = scenario.getHubId();
        String scenarioName = scenario.getName();

        for (Map.Entry<String, Action> entry : scenario.getActions().entrySet()) {
            log.info("== Iteration for entry = {}", entry);

            Instant timestamp = Instant.now();
            DeviceActionProto.Builder builder = DeviceActionProto.newBuilder()
                    .setSensorId(entry.getKey())
                    .setType(ActionTypeProto.valueOf(entry.getValue().getType().name()));

            if (entry.getValue().getType().equals(DeviceActionTypeAvro.SET_VALUE)) {
                builder.setValue(entry.getValue().getValue());
            }
            DeviceActionProto deviceAction = builder.build();

            DeviceActionRequest request = DeviceActionRequest.newBuilder()
                    .setHubId(hubId)
                    .setScenarioName(scenarioName)
                    .setTimestamp(Timestamp.newBuilder()
                            .setSeconds(timestamp.getEpochSecond())
                            .setNanos(timestamp.getNano()))
                    .setActionEntity(deviceAction)
                    .build();

            scenarioExecutor.send(request);
            log.info("<== Send action by scenario with request = {}", request);
        }
    }
}
