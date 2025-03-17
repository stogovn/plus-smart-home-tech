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
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.service.HubService;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor implements Runnable {

    @Qualifier("snapshotService")
    private final HubService<SensorsSnapshotAvro> hubService;
    private final KafkaProperties kafkaProperties;

    @Value("${kafka.topic.snapshot.name}")
    private String topic;

    @Override
    public void run() {
        try (KafkaConsumer<Void, SensorsSnapshotAvro> consumer = new KafkaConsumer<>(
                kafkaProperties.getSnapshotsProperties())) {
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

            consumer.subscribe(List.of(topic));

            while (true) {
                ConsumerRecords<Void, SensorsSnapshotAvro> records = consumer.poll(
                        Duration.ofMillis(kafkaProperties.getConsumeAttemptTimeout()));
                if (!records.isEmpty()) {
                    for (ConsumerRecord<Void, SensorsSnapshotAvro> record : records) {
                        hubService.process(record.value());
                    }
                }
                consumer.commitSync();
            }
        } catch (WakeupException ignored) {
        } catch (Exception e) {
            log.error("Ошибка во время обработки снапшотов", e);
        }
    }
}
