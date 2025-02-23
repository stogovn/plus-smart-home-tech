package ru.yandex.practicum.controller;

import jakarta.validation.Valid;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.model.hub.HubEvent;
import ru.yandex.practicum.service.HubService;


@RestController
@RequestMapping("/events")
@AllArgsConstructor
public class HubController {

    private HubService hubService;

    @PostMapping("/hubs")
    public void collectHubEvent(@Valid @RequestBody HubEvent event) {
        hubService.processHubEvent(event);
    }
}