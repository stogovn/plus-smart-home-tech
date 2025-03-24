package ru.yandex.practicum.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.service.HubEventService;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {
    @Value("${kafka.hub-consumer.attempt-timeout}")
    int attemptTimeout;
    @Value("${kafka.hub-consumer.topics}")
    String topicTelemetryHubs;

    private final KafkaConsumer<String, HubEventAvro> consumer;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    private final HubEventService service;

    @Override
    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
        try {
            consumer.subscribe(List.of(topicTelemetryHubs));
            while (true) {
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(Duration.ofMillis(attemptTimeout));
                int count = 0;
                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    handleRecord(record);
                    manageOffsets(record, count, consumer);
                    count++;
                }
                consumer.commitAsync();
            }

        } catch (WakeupException ignores) {

        } catch (Exception e) {
            log.warn("HubEvent processor have got an error", e);
        } finally {

            try {
                consumer.commitSync(currentOffsets);

            } finally {
                log.info("HubEvent consumer is closing");
                consumer.close();
            }
        }
    }

    private void handleRecord(ConsumerRecord<String, HubEventAvro> consumerRecord) throws InterruptedException {
        log.info("handleRecord {}", consumerRecord);
        service.process(consumerRecord.value());
    }

    private void manageOffsets(ConsumerRecord<String, HubEventAvro> consumerRecord,
                               int count,
                               Consumer<String, HubEventAvro> consumer) {
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
