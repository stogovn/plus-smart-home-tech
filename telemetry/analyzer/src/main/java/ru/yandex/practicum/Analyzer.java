package ru.yandex.practicum;

import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.processor.HubEventProcessor;
import ru.yandex.practicum.processor.SnapshotProcessor;

@Slf4j
@SpringBootApplication
public class Analyzer {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(Analyzer.class, args);

        final HubEventProcessor hubEventProcessor = context.getBean(HubEventProcessor.class);
        final SnapshotProcessor snapshotProcessor = context.getBean(SnapshotProcessor.class);

        CompletableFuture.runAsync(() -> {
                    log.info("Запускается hubEventProcessor");
                    hubEventProcessor.run();
                }
        ).whenComplete((res, ex) -> {
            if (ex != null) {
                log.error(ex.getMessage(), ex);
            }
        });
        CompletableFuture.runAsync(() -> {
                    log.info("Запускается snapshotProcessor");
                    snapshotProcessor.run();
                }
        ).whenComplete((res, ex) -> {
            if (ex != null) {
                log.error(ex.getMessage(), ex);
            }
        });
    }
}