package ru.yandex.practicum.service;

import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.model.Scenario;

import java.util.List;

public interface AnalyzerService {

    List<Scenario> getScenariosBySnapshot(SensorsSnapshotAvro sensorsSnapshotAvro);
}
