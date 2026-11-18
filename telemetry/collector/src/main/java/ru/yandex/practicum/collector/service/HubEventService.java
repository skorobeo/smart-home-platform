package ru.yandex.practicum.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.config.KafkaTopics;
import ru.yandex.practicum.collector.dto.hub.HubEvent;
import ru.yandex.practicum.collector.mapper.HubEventMapper;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

@Slf4j
@Service
@RequiredArgsConstructor
public class HubEventService {

    private final KafkaTemplate<String, SpecificRecord> kafkaTemplate;
    private final HubEventMapper hubEventMapper;
    private final KafkaTopics kafkaTopics;

    public void collectHubEvent(HubEvent event) {
        sendHubEvent(event);
    }

    /**
     * Отправляет событие хаба в Kafka и возвращает future, который завершается
     * после подтверждения записи брокером.
     */
    public CompletableFuture<Void> sendHubEvent(HubEvent event) {
        HubEventAvro avro = hubEventMapper.toAvro(event);
        String topic = kafkaTopics.getHubs();
        return kafkaTemplate.send(topic, event.getHubId(), avro)
                .thenAccept(result ->
                        log.info("Событие хаба {} отправлено в топик {}", event.getHubId(), topic))
                .exceptionally(exception -> {
                    log.error("Не удалось отправить событие хаба {} в топик {}",
                            event.getHubId(), topic, exception);
                    throw new CompletionException(exception);
                });
    }
}
