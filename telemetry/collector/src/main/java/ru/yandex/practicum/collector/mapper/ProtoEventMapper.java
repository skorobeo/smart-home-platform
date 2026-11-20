package ru.yandex.practicum.collector.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.dto.hub.ActionType;
import ru.yandex.practicum.collector.dto.hub.ConditionOperation;
import ru.yandex.practicum.collector.dto.hub.ConditionType;
import ru.yandex.practicum.collector.dto.hub.DeviceAction;
import ru.yandex.practicum.collector.dto.hub.DeviceAddedEvent;
import ru.yandex.practicum.collector.dto.hub.DeviceRemovedEvent;
import ru.yandex.practicum.collector.dto.hub.DeviceType;
import ru.yandex.practicum.collector.dto.hub.HubEvent;
import ru.yandex.practicum.collector.dto.hub.ScenarioAddedEvent;
import ru.yandex.practicum.collector.dto.hub.ScenarioCondition;
import ru.yandex.practicum.collector.dto.hub.ScenarioRemovedEvent;
import ru.yandex.practicum.collector.dto.sensor.ClimateSensorEvent;
import ru.yandex.practicum.collector.dto.sensor.LightSensorEvent;
import ru.yandex.practicum.collector.dto.sensor.MotionSensorEvent;
import ru.yandex.practicum.collector.dto.sensor.SensorEvent;
import ru.yandex.practicum.collector.dto.sensor.SwitchSensorEvent;
import ru.yandex.practicum.collector.dto.sensor.TemperatureSensorEvent;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

import java.time.Instant;
import java.util.List;

@Component
public class ProtoEventMapper {

    public SensorEvent toDto(SensorEventProto event) {
        SensorEvent result = switch (event.getPayloadCase()) {
            case MOTION_SENSOR -> {
                MotionSensorEvent motion = new MotionSensorEvent();
                motion.setLinkQuality(event.getMotionSensor().getLinkQuality());
                motion.setMotion(event.getMotionSensor().getMotion());
                motion.setVoltage(event.getMotionSensor().getVoltage());
                yield motion;
            }
            case TEMPERATURE_SENSOR -> {
                TemperatureSensorEvent temperature = new TemperatureSensorEvent();
                temperature.setTemperatureC(event.getTemperatureSensor().getTemperatureC());
                temperature.setTemperatureF(event.getTemperatureSensor().getTemperatureF());
                yield temperature;
            }
            case LIGHT_SENSOR -> {
                LightSensorEvent light = new LightSensorEvent();
                light.setLinkQuality(event.getLightSensor().getLinkQuality());
                light.setLuminosity(event.getLightSensor().getLuminosity());
                yield light;
            }
            case CLIMATE_SENSOR -> {
                ClimateSensorEvent climate = new ClimateSensorEvent();
                climate.setTemperatureC(event.getClimateSensor().getTemperatureC());
                climate.setHumidity(event.getClimateSensor().getHumidity());
                climate.setCo2Level(event.getClimateSensor().getCo2Level());
                yield climate;
            }
            case SWITCH_SENSOR -> {
                SwitchSensorEvent smartSwitch = new SwitchSensorEvent();
                smartSwitch.setState(event.getSwitchSensor().getState());
                yield smartSwitch;
            }
            case PAYLOAD_NOT_SET -> throw new IllegalArgumentException(
                    "Не задан payload события датчика " + event.getId());
        };

        result.setId(event.getId());
        result.setHubId(event.getHubId());
        result.setTimestamp(toInstant(event.getTimestamp()));
        return result;
    }

    public HubEvent toDto(HubEventProto event) {
        HubEvent result = switch (event.getPayloadCase()) {
            case DEVICE_ADDED -> {
                DeviceAddedEvent added = new DeviceAddedEvent();
                added.setId(event.getDeviceAdded().getId());
                added.setDeviceType(DeviceType.valueOf(event.getDeviceAdded().getType().name()));
                yield added;
            }
            case DEVICE_REMOVED -> {
                DeviceRemovedEvent removed = new DeviceRemovedEvent();
                removed.setId(event.getDeviceRemoved().getId());
                yield removed;
            }
            case SCENARIO_ADDED -> toScenarioAdded(event.getScenarioAdded());
            case SCENARIO_REMOVED -> {
                ScenarioRemovedEvent removed = new ScenarioRemovedEvent();
                removed.setName(event.getScenarioRemoved().getName());
                yield removed;
            }
            case PAYLOAD_NOT_SET -> throw new IllegalArgumentException(
                    "Не задан payload события хаба " + event.getHubId());
        };

        result.setHubId(event.getHubId());
        result.setTimestamp(toInstant(event.getTimestamp()));
        return result;
    }

    private ScenarioAddedEvent toScenarioAdded(ScenarioAddedEventProto proto) {
        ScenarioAddedEvent event = new ScenarioAddedEvent();
        event.setName(proto.getName());
        event.setConditions(mapConditions(proto.getConditionList()));
        event.setActions(mapActions(proto.getActionList()));
        return event;
    }

    private List<ScenarioCondition> mapConditions(List<ScenarioConditionProto> conditions) {
        return conditions.stream()
                .map(proto -> {
                    ScenarioCondition condition = new ScenarioCondition();
                    condition.setSensorId(proto.getSensorId());
                    condition.setType(ConditionType.valueOf(proto.getType().name()));
                    condition.setOperation(ConditionOperation.valueOf(proto.getOperation().name()));
                    condition.setValue(mapConditionValue(proto));
                    return condition;
                })
                .toList();
    }

    private Integer mapConditionValue(ScenarioConditionProto proto) {
        if (proto.hasIntValue()) {
            return proto.getIntValue();
        }
        if (proto.hasBoolValue()) {
            return proto.getBoolValue() ? 1 : 0;
        }
        return null;
    }

    private List<DeviceAction> mapActions(List<ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto> actions) {
        return actions.stream()
                .map(proto -> {
                    DeviceAction action = new DeviceAction();
                    action.setSensorId(proto.getSensorId());
                    action.setType(ActionType.valueOf(proto.getType().name()));
                    action.setValue(proto.hasValue() ? proto.getValue() : null);
                    return action;
                })
                .toList();
    }

    private Instant toInstant(com.google.protobuf.Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}
