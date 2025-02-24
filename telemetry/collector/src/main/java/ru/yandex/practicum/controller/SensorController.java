package ru.yandex.practicum.controller;

import jakarta.validation.Valid;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.model.sensors.SensorEvent;
import ru.yandex.practicum.service.SensorService;

@Slf4j
@RestController
@RequestMapping("/events")
@AllArgsConstructor
public class SensorController {

    private SensorService sensorService;

    @PostMapping("/sensors")
    public void collectSensorEvent(@Valid @RequestBody SensorEvent event) {
        log.info("Получено событие sensor: {}", event);
        sensorService.processSensorEvent(event);
    }
}