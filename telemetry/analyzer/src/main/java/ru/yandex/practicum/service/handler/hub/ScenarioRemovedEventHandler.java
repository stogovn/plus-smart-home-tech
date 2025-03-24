package ru.yandex.practicum.service.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.repository.ScenarioRepository;

@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioRemovedEventHandler implements HubEventHandler {
    private final ScenarioRepository scenarioRepository;

    @Override
    public String getEventType() {
        return ScenarioRemovedEventAvro.class.getName();
    }

    @Transactional
    @Override
    public void handle(HubEventAvro hubEvent) {
        String scenarioName = ((ScenarioRemovedEventAvro) hubEvent.getPayload()).getName();
        log.info("Handle delete scenario: {}", scenarioName);
        scenarioRepository.deleteByHubIdAndName(hubEvent.getHubId(), scenarioName);
    }
}
