package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.model.hub.DeviceAddedEvent;
import ru.yandex.practicum.model.hub.DeviceRemovedEvent;
import ru.yandex.practicum.model.hub.HubEvent;
import ru.yandex.practicum.model.hub.ScenarioAddedEvent;
import ru.yandex.practicum.model.hub.ScenarioRemovedEvent;
import ru.yandex.practicum.service.converter.hub.DeviceAddedEventConverter;
import ru.yandex.practicum.service.converter.hub.DeviceRemovedEventConverter;
import ru.yandex.practicum.service.converter.hub.ScenarioAddedEventConverter;
import ru.yandex.practicum.service.converter.hub.ScenarioRemovedEventConverter;

@Slf4j
@Service
@RequiredArgsConstructor
public class HubService {

    private final KafkaTemplate<String, Object> hubKafkaTemplate;

    @Value("${hubEventTopic}")
    private String hubEventTopic;

    public void processHubEvent(HubEvent event) {
        switch (event.getType()) {
            case DEVICE_ADDED:
                new DeviceAddedEventConverter()
                        .sendEvent(hubKafkaTemplate, hubEventTopic, event.getHubId(), (DeviceAddedEvent) event);
                break;
            case DEVICE_REMOVED:
                new DeviceRemovedEventConverter()
                        .sendEvent(hubKafkaTemplate, hubEventTopic, event.getHubId(), (DeviceRemovedEvent) event);
                break;
            case SCENARIO_ADDED:
                new ScenarioAddedEventConverter()
                        .sendEvent(hubKafkaTemplate, hubEventTopic, event.getHubId(), (ScenarioAddedEvent) event);
                break;
            case SCENARIO_REMOVED:
                new ScenarioRemovedEventConverter()
                        .sendEvent(hubKafkaTemplate, hubEventTopic, event.getHubId(), (ScenarioRemovedEvent) event);
                break;
            default:
                throw new IllegalArgumentException("Неизвестный тип события hub: " + event.getType());
        }
    }
}