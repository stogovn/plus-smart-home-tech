package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.model.hub.HubEvent;

@Service
@RequiredArgsConstructor
public class HubService {

    private final KafkaProducerService kafkaProducerService;

    public void processHubEvent(HubEvent event) {
        kafkaProducerService.sendHubEvent(event);
    }

}