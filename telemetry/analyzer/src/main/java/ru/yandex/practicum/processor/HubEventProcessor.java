package ru.yandex.practicum.processor;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.time.Duration;
import java.util.Collections;

@Slf4j
@Component
public class HubEventProcessor implements Runnable {
    private final KafkaConsumer<String, HubEventAvro> consumer;

    @Value("${hubEventTopic}")
    private String hubEventTopic;


    public HubEventProcessor(KafkaConsumer<String, HubEventAvro> consumer) {
        this.consumer = consumer;
    }

    @PostConstruct
    public void init() {

        Thread processorThread = new Thread(this, "HubEventProcessorThread");
        processorThread.start();
        log.info("HubEventProcessor запущен в отдельном потоке.");
    }

    @Override
    public void run() {
        try {
            // Подписываемся на топик с событиями от хаба
            consumer.subscribe(Collections.singletonList(hubEventTopic));
            log.info("Подписка на топик {}", hubEventTopic);
            while (!Thread.currentThread().isInterrupted()) {
                // Опрос топика каждые 1 секунду
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    HubEventAvro event = record.value();
                    log.info("Получено событие из Kafka: {}", event);
                    processHubEvent(event);
                }
                // Фиксируем оффсеты после обработки записей
                consumer.commitSync();
            }
        } catch (Exception e) {
            log.error("Ошибка в цикле опроса HubEventProcessor", e);
        } finally {
            consumer.close();
            log.info("Kafka consumer закрыт.");
        }
    }

    /**
     * Метод обработки события HubEventAvro.
     */
    private void processHubEvent(HubEventAvro event) {
        log.info("Обработка события HubEventAvro: {}", event);

    }
}
