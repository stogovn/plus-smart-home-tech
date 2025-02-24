package ru.yandex.practicum.controller;

import jakarta.validation.Valid;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.model.hub.HubEvent;
import ru.yandex.practicum.service.HubService;

@Slf4j
@RestController
@RequestMapping("/events")
@AllArgsConstructor
public class HubController {

    private HubService hubService;

    @PostMapping("/hubs")
    public void collectHubEvent(@Valid @RequestBody HubEvent event) {
        log.info("Получено событие hub: {}", event);
        hubService.processHubEvent(event);
    }
}