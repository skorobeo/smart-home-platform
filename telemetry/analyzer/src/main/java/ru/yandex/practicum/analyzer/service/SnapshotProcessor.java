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
import ru.yandex.practicum.analyzer.dto.ScenarioDefinition;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor implements Runnable {

    private final AnalyzerProperties properties;
    private final AnalyzerConsumerFactory consumerFactory;
    private final ScenarioService scenarioService;
    private final ScenarioEvaluator scenarioEvaluator;
    private final DeviceActionSender deviceActionSender;

    private volatile boolean running = true;
    private KafkaConsumer<String, SensorsSnapshotAvro> consumer;

    @Override
    public void run() {
        start();
    }

    public void start() {
        consumer = consumerFactory.createSnapshotConsumer();
        consumer.subscribe(List.of(properties.getTopics().getSnapshots()));
        log.info("Запущена обработка снапшотов из топика {}", properties.getTopics().getSnapshots());

        try {
            while (running) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = consumer.poll(
                        Duration.ofMillis(properties.getSnapshotConsumer().getPollTimeout()));
                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    process(record);
                }
            }
        } catch (WakeupException e) {
            if (running) {
                throw e;
            }
            log.info("Остановка обработчика снапшотов");
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

    private void process(ConsumerRecord<String, SensorsSnapshotAvro> record) {
        SensorsSnapshotAvro snapshot = record.value();
        if (snapshot == null) {
            commit(record);
            return;
        }

        try {
            List<ScenarioDefinition> scenarios = scenarioService.findDefinitions(snapshot.getHubId());
            if (!scenarios.isEmpty()) {
                List<DeviceActionRequest> requests = scenarioEvaluator.evaluate(snapshot, scenarios);
                for (DeviceActionRequest request : requests) {
                    deviceActionSender.send(request);
                }
                if (!requests.isEmpty()) {
                    log.info("Снапшот хаба {} обработан, сценариев выполнено: {}",
                            snapshot.getHubId(), requests.size());
                }
            }
        } catch (Exception e) {
            log.error("Не удалось обработать снапшот хаба {}", snapshot.getHubId(), e);
        } finally {
            // Смещение фиксируем после обработки, чтобы не обрабатывать снапшот повторно
            // при перезапуске сервиса.
            commit(record);
        }
    }

    private void commit(ConsumerRecord<String, SensorsSnapshotAvro> record) {
        consumer.commitSync(Map.of(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1)));
    }
}
