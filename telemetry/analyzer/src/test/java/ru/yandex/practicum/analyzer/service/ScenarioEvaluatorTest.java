package ru.yandex.practicum.analyzer.service;

import org.junit.jupiter.api.Test;
import ru.yandex.practicum.analyzer.dto.ActionDefinition;
import ru.yandex.practicum.analyzer.dto.ConditionDefinition;
import ru.yandex.practicum.analyzer.dto.ScenarioDefinition;
import ru.yandex.practicum.analyzer.model.ActionType;
import ru.yandex.practicum.analyzer.model.ConditionOperation;
import ru.yandex.practicum.analyzer.model.ConditionType;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ScenarioEvaluatorTest {

    private final ScenarioEvaluator evaluator = new ScenarioEvaluator();

    @Test
    void shouldTriggerScenarioWhenAllConditionsMatch() {
        SensorsSnapshotAvro snapshot = snapshot(
                climateState(10),
                lightState(100));

        ScenarioDefinition scenario = new ScenarioDefinition(
                "hub-1",
                "Тёплый пол",
                List.of(
                        new ConditionDefinition("climate-1", ConditionType.TEMPERATURE,
                                ConditionOperation.LOWER_THAN, 15),
                        new ConditionDefinition("light-1", ConditionType.LUMINOSITY,
                                ConditionOperation.LOWER_THAN, 500)),
                List.of(new ActionDefinition("socket-1", ActionType.ACTIVATE, null)));

        List<DeviceActionRequest> requests = evaluator.evaluate(snapshot, List.of(scenario));

        assertThat(requests).hasSize(1);
        DeviceActionRequest request = requests.getFirst();
        assertThat(request.getHubId()).isEqualTo("hub-1");
        assertThat(request.getScenarioName()).isEqualTo("Тёплый пол");
        assertThat(request.getAction().getSensorId()).isEqualTo("socket-1");
        assertThat(request.getAction().getType().name()).isEqualTo("ACTIVATE");
        assertThat(request.getAction().hasValue()).isFalse();
    }

    @Test
    void shouldNotTriggerScenarioWhenAnyConditionDoesNotMatch() {
        SensorsSnapshotAvro snapshot = snapshot(
                climateState(20),
                lightState(100));

        ScenarioDefinition scenario = new ScenarioDefinition(
                "hub-1",
                "Тёплый пол",
                List.of(
                        new ConditionDefinition("climate-1", ConditionType.TEMPERATURE,
                                ConditionOperation.LOWER_THAN, 15),
                        new ConditionDefinition("light-1", ConditionType.LUMINOSITY,
                                ConditionOperation.LOWER_THAN, 500)),
                List.of(new ActionDefinition("socket-1", ActionType.ACTIVATE, null)));

        List<DeviceActionRequest> requests = evaluator.evaluate(snapshot, List.of(scenario));

        assertThat(requests).isEmpty();
    }

    private SensorsSnapshotAvro snapshot(SensorStateAvro climate, SensorStateAvro light) {
        return SensorsSnapshotAvro.newBuilder()
                .setHubId("hub-1")
                .setTimestamp(Instant.now())
                .setSensorsState(Map.of(
                        "climate-1", climate,
                        "light-1", light))
                .build();
    }

    private SensorStateAvro climateState(int temperature) {
        return SensorStateAvro.newBuilder()
                .setTimestamp(Instant.now())
                .setData(ClimateSensorAvro.newBuilder()
                        .setTemperatureC(temperature)
                        .setHumidity(50)
                        .setCo2Level(700)
                        .build())
                .build();
    }

    private SensorStateAvro lightState(int luminosity) {
        return SensorStateAvro.newBuilder()
                .setTimestamp(Instant.now())
                .setData(LightSensorAvro.newBuilder()
                        .setLinkQuality(50)
                        .setLuminosity(luminosity)
                        .build())
                .build();
    }
}
