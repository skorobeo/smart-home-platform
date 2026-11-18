package ru.yandex.practicum.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.config.KafkaTopics;
import ru.yandex.practicum.collector.dto.sensor.SensorEvent;
import ru.yandex.practicum.collector.mapper.SensorEventMapper;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

@Slf4j
@Service
@RequiredArgsConstructor
public class SensorEventService {

    private final KafkaTemplate<String, SpecificRecord> kafkaTemplate;
    private final SensorEventMapper sensorEventMapper;
    private final KafkaTopics kafkaTopics;

    public void collectSensorEvent(SensorEvent event) {
        sendSensorEvent(event);
    }

    /**
     * Отправляет событие датчика в Kafka и возвращает future, который завершается
     * после подтверждения записи брокером. gRPC-контроллер использует его, чтобы
     * не подтверждать вызов хабу раньше фактического сохранения события.
     */
    public CompletableFuture<Void> sendSensorEvent(SensorEvent event) {
        SensorEventAvro avro = sensorEventMapper.toAvro(event);
        String topic = kafkaTopics.getSensors();
        return kafkaTemplate.send(topic, event.getId(), avro)
                .thenAccept(result ->
                        log.info("Событие датчика {} отправлено в топик {}", event.getId(), topic))
                .exceptionally(exception -> {
                    log.error("Не удалось отправить событие датчика {} в топик {}",
                            event.getId(), topic, exception);
                    throw new CompletionException(exception);
                });
    }
}
