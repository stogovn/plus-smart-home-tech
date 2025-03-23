package ru.yandex.practicum.service;

import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.model.Scenario;

public interface HubEventService {
    void process(HubEventAvro hubEventAvro);

    void sendActionsByScenario(Scenario scenario);
}
