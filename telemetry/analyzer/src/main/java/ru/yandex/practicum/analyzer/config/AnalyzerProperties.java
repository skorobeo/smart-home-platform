package ru.yandex.practicum.analyzer.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@ConfigurationProperties(prefix = "analyzer.kafka")
public class AnalyzerProperties {

    private String bootstrapServers = "localhost:9092";
    private long retryDelayMs = 1000;
    private Topics topics = new Topics();
    private Consumer snapshotConsumer = new Consumer();
    private Consumer hubConsumer = new Consumer();

    @Getter
    @Setter
    public static class Topics {
        private String snapshots;
        private String hubs;
    }

    @Getter
    @Setter
    public static class Consumer {
        private String groupId;
        private String clientId;
        private String autoOffsetReset = "earliest";
        private boolean enableAutoCommit = false;
        private int maxPollRecords = 100;
        private long pollTimeout = 1000;
        private Map<String, String> properties = new HashMap<>();
    }
}
