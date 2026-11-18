package ru.yandex.practicum.analyzer.service;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.config.AnalyzerProperties;
import ru.yandex.practicum.kafka.deserializer.HubEventDeserializer;
import ru.yandex.practicum.kafka.deserializer.SensorsSnapshotDeserializer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

@Component
@RequiredArgsConstructor
public class AnalyzerConsumerFactory {

    private final AnalyzerProperties properties;

    public KafkaConsumer<String, SensorsSnapshotAvro> createSnapshotConsumer() {
        return new KafkaConsumer<>(consumerProperties(
                properties.getSnapshotConsumer(), SensorsSnapshotDeserializer.class));
    }

    public KafkaConsumer<String, HubEventAvro> createHubConsumer() {
        return new KafkaConsumer<>(consumerProperties(
                properties.getHubConsumer(), HubEventDeserializer.class));
    }

    private Properties consumerProperties(AnalyzerProperties.Consumer consumer,
                                          Class<?> valueDeserializer) {
        Properties result = new Properties();
        result.put(BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        result.put(GROUP_ID_CONFIG, consumer.getGroupId());
        result.put(CLIENT_ID_CONFIG, consumer.getClientId());
        result.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        result.put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getName());
        result.put(ENABLE_AUTO_COMMIT_CONFIG, consumer.isEnableAutoCommit());
        result.put(AUTO_OFFSET_RESET_CONFIG, consumer.getAutoOffsetReset());
        result.put(MAX_POLL_RECORDS_CONFIG, consumer.getMaxPollRecords());
        result.putAll(consumer.getProperties());
        return result;
    }
}
