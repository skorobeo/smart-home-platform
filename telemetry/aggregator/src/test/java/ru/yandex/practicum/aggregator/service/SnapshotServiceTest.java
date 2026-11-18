package ru.yandex.practicum.aggregator.service;

import org.junit.jupiter.api.Test;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class SnapshotServiceTest {

    private final SnapshotService snapshotService = new SnapshotService();

    @Test
    void shouldCreateSnapshotForFirstEvent() {
        SensorEventAvro event = lightEvent(100, "2024-01-01T00:00:00Z");

        Optional<SensorsSnapshotAvro> snapshot = snapshotService.prepare(event);

        assertThat(snapshot).isPresent();
        assertThat(snapshot.get().getHubId()).isEqualTo("hub-1");
        assertThat(snapshot.get().getSensorsState()).containsKey("sensor-1");
    }

    @Test
    void shouldNotCreateSnapshotWhenDataDidNotChange() {
        snapshotService.confirm(snapshotService.prepare(lightEvent(100, "2024-01-01T00:00:00Z")).orElseThrow());

        Optional<SensorsSnapshotAvro> snapshot =
                snapshotService.prepare(lightEvent(100, "2024-01-01T00:01:00Z"));

        assertThat(snapshot).isEmpty();
    }

    @Test
    void shouldIgnoreOlderEventWithDifferentData() {
        snapshotService.confirm(snapshotService.prepare(lightEvent(100, "2024-01-01T00:02:00Z")).orElseThrow());

        Optional<SensorsSnapshotAvro> snapshot =
                snapshotService.prepare(lightEvent(200, "2024-01-01T00:01:00Z"));

        assertThat(snapshot).isEmpty();
    }

    @Test
    void shouldUpdateSnapshotForNewerEventWithDifferentData() {
        snapshotService.confirm(snapshotService.prepare(lightEvent(100, "2024-01-01T00:00:00Z")).orElseThrow());

        Optional<SensorsSnapshotAvro> snapshot =
                snapshotService.prepare(lightEvent(200, "2024-01-01T00:01:00Z"));

        assertThat(snapshot).isPresent();
        assertThat(snapshot.get().getTimestamp())
                .isEqualTo(Instant.parse("2024-01-01T00:01:00Z"));
    }

    private SensorEventAvro lightEvent(int luminosity, String timestamp) {
        return SensorEventAvro.newBuilder()
                .setId("sensor-1")
                .setHubId("hub-1")
                .setTimestamp(Instant.parse(timestamp))
                .setPayload(LightSensorAvro.newBuilder()
                        .setLinkQuality(10)
                        .setLuminosity(luminosity)
                        .build())
                .build();
    }
}
