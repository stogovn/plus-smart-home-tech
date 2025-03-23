package ru.yandex.practicum.service;

import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

public class SensorsSnapshotDeserializer extends BaseAvroDeserializer<SensorsSnapshotAvro> {
    public SensorsSnapshotDeserializer() {
        super(SensorsSnapshotAvro.getClassSchema());
    }
}
