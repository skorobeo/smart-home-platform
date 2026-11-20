package ru.yandex.practicum.aggregator.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Slf4j
@Service
public class SnapshotService {

    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    /**
     * Готовит новый снапшот, но не сохраняет его в памяти. Сохранение выполняется
     * отдельно после успешной отправки в Kafka, чтобы при ошибке событие можно было
     * обработать повторно без потери состояния.
     */
    public synchronized Optional<SensorsSnapshotAvro> prepare(SensorEventAvro event) {
        SensorsSnapshotAvro current = snapshots.get(event.getHubId());
        Map<String, SensorStateAvro> states = current == null
                ? new HashMap<>()
                : new HashMap<>(current.getSensorsState());

        SensorStateAvro previous = states.get(event.getId());
        if (!isUpdateNeeded(previous, event)) {
            return Optional.empty();
        }

        SensorStateAvro state = SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build();
        states.put(event.getId(), state);

        SensorsSnapshotAvro updated = SensorsSnapshotAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(state.getTimestamp())
                .setSensorsState(states)
                .build();
        return Optional.of(updated);
    }

    public synchronized void confirm(SensorsSnapshotAvro snapshot) {
        snapshots.put(snapshot.getHubId(), snapshot);
    }

    public synchronized Optional<SensorsSnapshotAvro> getSnapshot(String hubId) {
        return Optional.ofNullable(snapshots.get(hubId));
    }

    private boolean isUpdateNeeded(SensorStateAvro previous, SensorEventAvro event) {
        if (previous == null) {
            return true;
        }
        if (Objects.equals(previous.getData(), event.getPayload())) {
            return false;
        }
        Instant previousTimestamp = previous.getTimestamp().truncatedTo(ChronoUnit.MILLIS);
        Instant eventTimestamp = event.getTimestamp().truncatedTo(ChronoUnit.MILLIS);
        return !previousTimestamp.isAfter(eventTimestamp);
    }
}
