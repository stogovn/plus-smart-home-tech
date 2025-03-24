package ru.yandex.practicum.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Component
@RequiredArgsConstructor
@Slf4j
public class AggregationStarter {
    private final Duration consumeAttemptTimeout = Duration.ofMillis(1000);
    @Value("${topic.telemetry.sensors}")
    private String topicTelemetrySensors;
    @Value("${topic.telemetry.snapshots}")
    private String topicTelemetrySnapshots;

    private final KafkaConsumer<String, SensorEventAvro> kafkaConsumer;
    private final KafkaProducer<String, SensorsSnapshotAvro> kafkaProducer;
    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    public void start() {
        log.info("AggregationStarter запущен. Подписываемся на топик: {}", topicTelemetrySensors);
        try {
            kafkaConsumer.subscribe(List.of(topicTelemetrySensors));
            log.info("Подписка на топик {} выполнена.", topicTelemetrySensors);

            while (true) {
                ConsumerRecords<String, SensorEventAvro> records = kafkaConsumer.poll(consumeAttemptTimeout);
                log.debug("Получено {} записей", records.count());

                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    log.info("Обработка записи. Partition = {}, Offset = {}", record.partition(), record.offset());
                    SensorEventAvro event = record.value();
                    log.debug("Получено событие: {}", event);

                    Optional<SensorsSnapshotAvro> sensorsSnapshotAvro = updateState(event);
                    if (sensorsSnapshotAvro.isPresent()) {
                        SensorsSnapshotAvro snapshotAvro = sensorsSnapshotAvro.get();
                        ProducerRecord<String, SensorsSnapshotAvro> producerRecord =
                                new ProducerRecord<>(topicTelemetrySnapshots,
                                        null,
                                        snapshotAvro.getTimestamp().toEpochMilli(),
                                        snapshotAvro.getHubId(),
                                        snapshotAvro);
                        kafkaProducer.send(producerRecord, (metadata, exception) -> {
                            if (exception == null) {
                                log.info("Снимок отправлен: topic={}, partition={}, offset={}",
                                        metadata.topic(), metadata.partition(), metadata.offset());
                            } else {
                                log.error("Ошибка отправки снимка: {}", exception.getMessage(), exception);
                            }
                        });
                    } else {
                        log.debug("Для события с Offset {} не требуется обновление состояния", record.offset());
                    }
                }
                try {
                    kafkaConsumer.commitSync();
                    log.debug("Оффсеты успешно зафиксированы после обработки партии сообщений.");
                } catch (Exception commitEx) {
                    log.error("Ошибка фиксации оффсетов: {}", commitEx.getMessage(), commitEx);
                }
            }

        } catch (WakeupException e) {
            log.info("Получен WakeupException — завершаем работу consumer.");
        } catch (Exception e) {
            log.error("Ошибка при обработке sensor events: {}", e.getMessage(), e);
        } finally {
            try {
                log.info("Финализация: ожидание завершения отправки сообщений producer...");
                kafkaProducer.flush();
                log.info("Фиксация оффсетов при завершении работы.");
                kafkaConsumer.commitSync();
            } catch (Exception finalEx) {
                log.error("Ошибка при финальной фиксации оффсетов: {}", finalEx.getMessage(), finalEx);
            } finally {
                log.info("Закрытие consumer...");
                kafkaConsumer.close();
                log.info("Закрытие producer...");
                kafkaProducer.close();
            }
        }
    }

    private Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        log.info("==> Update state for event: {}", event);

        String hubId = event.getHubId();
        String eventId = event.getId();

        SensorsSnapshotAvro snapshot = snapshots.computeIfAbsent(hubId, s -> {
            SensorsSnapshotAvro newSnapshot = new SensorsSnapshotAvro();
            newSnapshot.setHubId(hubId);
            newSnapshot.setTimestamp(event.getTimestamp());
            newSnapshot.setSensorsState(new HashMap<>());
            return newSnapshot;
        });

        Map<String, SensorStateAvro> sensorsState = snapshot.getSensorsState();
        SensorStateAvro oldState = sensorsState.get(eventId);
        if (oldState != null && (oldState.getTimestamp().isAfter(event.getTimestamp())
                                 || oldState.getData().equals(event.getPayload()))) {
            return Optional.empty();
        }

        SensorStateAvro newState = new SensorStateAvro();
        newState.setTimestamp(event.getTimestamp());
        newState.setData(event.getPayload());
        sensorsState.put(eventId, newState);
        snapshot.setTimestamp(event.getTimestamp());

        log.info("<== Updated snapshot: {}", snapshot);
        return Optional.of(snapshot);
    }
}
