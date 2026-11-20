package ru.yandex.practicum.analyzer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.analyzer.service.HubEventProcessor;
import ru.yandex.practicum.analyzer.service.SnapshotProcessor;

@SpringBootApplication
@ConfigurationPropertiesScan
public class Analyzer {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(Analyzer.class, args);

        HubEventProcessor hubEventProcessor = context.getBean(HubEventProcessor.class);
        SnapshotProcessor snapshotProcessor = context.getBean(SnapshotProcessor.class);

        Thread hubEventsThread = new Thread(hubEventProcessor, "HubEventHandlerThread");
        Thread snapshotsThread = new Thread(snapshotProcessor, "SnapshotProcessorThread");
        hubEventsThread.start();
        snapshotsThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            snapshotProcessor.stop();
            hubEventProcessor.stop();
        }, "AnalyzerShutdownHook"));

        try {
            hubEventsThread.join();
            snapshotsThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            snapshotProcessor.stop();
            hubEventProcessor.stop();
        }
    }
}
