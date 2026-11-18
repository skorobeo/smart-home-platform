package ru.yandex.practicum.analyzer.service;

import com.google.protobuf.Timestamp;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.analyzer.dto.ActionDefinition;
import ru.yandex.practicum.analyzer.dto.ConditionDefinition;
import ru.yandex.practicum.analyzer.dto.ScenarioDefinition;
import ru.yandex.practicum.analyzer.model.ConditionOperation;
import ru.yandex.practicum.analyzer.model.ConditionType;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Service
public class ScenarioEvaluator {

    public List<DeviceActionRequest> evaluate(SensorsSnapshotAvro snapshot,
                                              List<ScenarioDefinition> scenarios) {
        Map<String, SensorStateAvro> states = snapshot.getSensorsState();
        Timestamp timestamp = toTimestamp(Instant.now());

        return scenarios.stream()
                .filter(scenario -> isTriggered(scenario, states))
                .flatMap(scenario -> scenario.actions().stream()
                        .map(action -> toRequest(snapshot.getHubId(), scenario.name(), action, timestamp)))
                .toList();
    }

    private boolean isTriggered(ScenarioDefinition scenario, Map<String, SensorStateAvro> states) {
        return scenario.conditions().stream()
                .allMatch(condition -> matches(condition, states));
    }

    private boolean matches(ConditionDefinition condition, Map<String, SensorStateAvro> states) {
        SensorStateAvro state = states.get(condition.sensorId());
        if (state == null || condition.value() == null) {
            return false;
        }

        Integer actual = extractValue(state, condition.type());
        if (actual == null) {
            return false;
        }

        return switch (condition.operation()) {
            case EQUALS -> actual.intValue() == condition.value().intValue();
            case GREATER_THAN -> actual > condition.value();
            case LOWER_THAN -> actual < condition.value();
        };
    }

    private Integer extractValue(SensorStateAvro state, ConditionType type) {
        Object data = state.getData();
        return switch (type) {
            case MOTION -> data instanceof MotionSensorAvro motion
                    ? (motion.getMotion() ? 1 : 0)
                    : null;
            case LUMINOSITY -> data instanceof LightSensorAvro light
                    ? light.getLuminosity()
                    : null;
            case SWITCH -> data instanceof SwitchSensorAvro smartSwitch
                    ? (smartSwitch.getState() ? 1 : 0)
                    : null;
            case TEMPERATURE -> {
                if (data instanceof ClimateSensorAvro climate) {
                    yield climate.getTemperatureC();
                }
                if (data instanceof TemperatureSensorAvro temperature) {
                    yield temperature.getTemperatureC();
                }
                yield null;
            }
            case CO2LEVEL -> data instanceof ClimateSensorAvro climate
                    ? climate.getCo2Level()
                    : null;
            case HUMIDITY -> data instanceof ClimateSensorAvro climate
                    ? climate.getHumidity()
                    : null;
        };
    }

    private DeviceActionRequest toRequest(String hubId, String scenarioName,
                                          ActionDefinition action, Timestamp timestamp) {
        DeviceActionProto.Builder actionBuilder = DeviceActionProto.newBuilder()
                .setSensorId(action.sensorId())
                .setType(ActionTypeProto.valueOf(action.type().name()));
        if (action.value() != null) {
            actionBuilder.setValue(action.value());
        }

        return DeviceActionRequest.newBuilder()
                .setHubId(hubId)
                .setScenarioName(scenarioName)
                .setAction(actionBuilder.build())
                .setTimestamp(timestamp)
                .build();
    }

    private Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}
