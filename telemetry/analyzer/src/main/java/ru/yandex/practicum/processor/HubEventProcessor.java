package ru.yandex.practicum.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.config.KafkaProperties;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.service.HubService;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {

    @Qualifier("eventService")
    private final HubService<HubEventAvro> hubService;
    private final KafkaProperties kafkaProperties;

    @Value("${kafka.topic.hub.name}")
    private String topic;

    @Override
    public void run() {
        log.info("HubEventProcessor запущен. Подписываемся на топик: {}", topic);
        try (KafkaConsumer<Void, HubEventAvro> consumer = new KafkaConsumer<>(kafkaProperties.getHubEventProperties())) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Получен сигнал завершения работы, вызываем wakeup() у consumer.");
                consumer.wakeup();
            }));

            consumer.subscribe(List.of(topic));
            log.info("Подписка на топик {} выполнена.", topic);

            while (true) {
                ConsumerRecords<Void, HubEventAvro> records = consumer.poll(
                        Duration.ofMillis(kafkaProperties.getConsumeAttemptTimeout()));
                log.debug("Получено {} записей", records.count());

                if (!records.isEmpty()) {
                    for (ConsumerRecord<Void, HubEventAvro> record : records) {
                        log.debug("Обработка записи: partition={}, offset={}, value={}",
                                record.partition(), record.offset(), record.value());
                        try {
                            hubService.process(record.value());
                            log.debug("Запись с offset {} успешно обработана", record.offset());
                        } catch (Exception ex) {
                            log.error("Ошибка обработки записи на offset {}: {}", record.offset(), ex.getMessage(), ex);
                        }
                    }
                } else {
                    log.debug("В текущем цикле опроса записей не найдено.");
                }
            }
        } catch (WakeupException e) {
            log.info("WakeupException получен — завершаем работу consumer.");
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий хаба", e);
        } finally {
            log.info("HubEventProcessor завершил работу.");
        }
    }
}
