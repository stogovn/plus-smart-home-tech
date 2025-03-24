package ru.yandex.practicum.service.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.model.Sensor;
import ru.yandex.practicum.repository.SensorRepository;
import ru.yandex.practicum.service.handler.Mapper;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeviceAddedEventHandler implements HubEventHandler {
    private final SensorRepository sensorRepository;

    @Override
    public String getEventType() {
        return DeviceAddedEventAvro.class.getName();
    }

    @Transactional
    @Override
    public void handle(HubEventAvro hubEvent) {
        Sensor sensor = Mapper.mapToSensor(hubEvent, (DeviceAddedEventAvro) hubEvent.getPayload());
        if (!sensorRepository.existsByIdInAndHubId(List.of(sensor.getId()), sensor.getHubId())) {
            sensorRepository.save(sensor);
            log.info("New sensor added: {}", sensor);
        }
    }
}
