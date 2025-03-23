package ru.yandex.practicum.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.service.AnalyzerService;
import ru.yandex.practicum.service.HubEventService;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor implements Runnable {
    @Value("${kafka.snapshot-consumer.attempt-timeout}")
    int attemptTimeout;
    @Value("${kafka.snapshot-consumer.topics}")
    String topicTelemetrySnapshots;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    private final Consumer<String, SensorsSnapshotAvro> consumer;
    private final AnalyzerService analyzerService;
    private final HubEventService hubEventService;

    @Override
    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
        try {
            consumer.subscribe(List.of(topicTelemetrySnapshots));
            while (true) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = consumer.poll(Duration.ofMillis(attemptTimeout));
                int count = 0;
                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    handleRecord(record);
                    manageOffsets(record, count, consumer);
                    count++;
                }
                consumer.commitAsync();
            }

        } catch (WakeupException ignores) {

        } catch (Exception e) {
            log.warn("Snapshot processor have got an error", e);
        } finally {

            try {
                consumer.commitSync(currentOffsets);
            } finally {
                log.info("Analyzer snapshot consumer is closing");
                consumer.close();
            }
        }
    }

    private void handleRecord(ConsumerRecord<String, SensorsSnapshotAvro> consumerRecord) throws InterruptedException {
        log.info("==> Analyzer handle record with value = {}, partition = {}, offset = {}",
                consumerRecord.value(), consumerRecord.partition(), consumerRecord.offset());
        List<Scenario> scenarios = analyzerService.getScenariosBySnapshot(consumerRecord.value());
        log.info("== Found {} scenarios for execute", scenarios.size());
        for (Scenario scenario : scenarios) {
            hubEventService.sendActionsByScenario(scenario);
        }
    }

    private void manageOffsets(ConsumerRecord<String, SensorsSnapshotAvro> consumerRecord,
                               int count,
                               Consumer<String, SensorsSnapshotAvro> consumer) {
        currentOffsets.put(
                new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                new OffsetAndMetadata(consumerRecord.offset() + 1)
        );

        if (count % 10 == 0) {
            consumer.commitAsync(currentOffsets, (offsets, exception) -> {
                if (exception != null) {
                    log.warn("Error while fixing offsets: {}", offsets, exception);
                }
            });
        }
    }
}
