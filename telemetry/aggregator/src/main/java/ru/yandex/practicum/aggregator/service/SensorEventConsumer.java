package ru.yandex.practicum.aggregator.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.aggregator.config.AggregatorProperties;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class SensorEventConsumer {

    private final SnapshotService snapshotService;
    private final KafkaTemplate<String, SpecificRecord> kafkaTemplate;
    private final AggregatorProperties properties;

    @KafkaListener(topics = "${aggregator.kafka.topics.sensors}")
    public void consume(ConsumerRecord<String, SensorEventAvro> record, Acknowledgment acknowledgment) {
        SensorEventAvro event = record.value();
        if (event == null) {
            acknowledgment.acknowledge();
            return;
        }

        snapshotService.prepare(event).ifPresent(snapshot -> {
            sendSnapshot(snapshot);
            snapshotService.confirm(snapshot);
        });
        acknowledgment.acknowledge();
    }

    private void sendSnapshot(SensorsSnapshotAvro snapshot) {
        try {
            kafkaTemplate.send(properties.getTopics().getSnapshots(), snapshot.getHubId(), snapshot)
                    .get(5, TimeUnit.SECONDS);
            log.info("Снапшот хаба {} отправлен в топик {}",
                    snapshot.getHubId(), properties.getTopics().getSnapshots());
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Не удалось отправить снапшот хаба " + snapshot.getHubId(), e);
        }
    }
}
