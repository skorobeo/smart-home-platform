package ru.yandex.practicum.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.config.AnalyzerProperties;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {

    private final AnalyzerProperties properties;
    private final AnalyzerConsumerFactory consumerFactory;
    private final ScenarioService scenarioService;

    private volatile boolean running = true;
    private KafkaConsumer<String, HubEventAvro> consumer;

    @Override
    public void run() {
        consumer = consumerFactory.createHubConsumer();
        consumer.subscribe(List.of(properties.getTopics().getHubs()));
        log.info("Запущена обработка событий хабов из топика {}", properties.getTopics().getHubs());

        try {
            while (running) {
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(
                        Duration.ofMillis(properties.getHubConsumer().getPollTimeout()));
                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    process(record);
                }
            }
        } catch (WakeupException e) {
            if (running) {
                throw e;
            }
            log.info("Остановка обработчика событий хабов");
        } finally {
            consumer.close();
        }
    }

    public void stop() {
        running = false;
        if (consumer != null) {
            consumer.wakeup();
        }
    }

    private void process(ConsumerRecord<String, HubEventAvro> record) {
        HubEventAvro event = record.value();
        if (event == null) {
            commit(record);
            return;
        }

        try {
            switch (event.getPayload()) {
                case DeviceAddedEventAvro deviceAdded ->
                        scenarioService.addDevice(event.getHubId(), deviceAdded.getId());
                case DeviceRemovedEventAvro deviceRemoved ->
                        scenarioService.removeDevice(event.getHubId(), deviceRemoved.getId());
                case ScenarioAddedEventAvro scenarioAdded ->
                        scenarioService.addScenario(event.getHubId(), scenarioAdded);
                case ScenarioRemovedEventAvro scenarioRemoved ->
                        scenarioService.removeScenario(event.getHubId(), scenarioRemoved.getName());
                default -> log.warn("Неизвестный тип события хаба {}", event.getPayload());
            }
        } catch (Exception e) {
            log.error("Не удалось обработать событие хаба {}", event.getHubId(), e);
        } finally {
            // События хабов повторно обрабатывать не опасно, но и задерживать их
            // в очереди нет смысла: операции добавления и удаления идемпотентны.
            commit(record);
        }
    }

    private void commit(ConsumerRecord<String, HubEventAvro> record) {
        consumer.commitSync(Map.of(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1)));
    }
}
