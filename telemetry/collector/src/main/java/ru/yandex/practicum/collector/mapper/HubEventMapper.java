package ru.yandex.practicum.collector.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.dto.hub.DeviceAddedEvent;
import ru.yandex.practicum.collector.dto.hub.DeviceAction;
import ru.yandex.practicum.collector.dto.hub.DeviceRemovedEvent;
import ru.yandex.practicum.collector.dto.hub.HubEvent;
import ru.yandex.practicum.collector.dto.hub.ScenarioAddedEvent;
import ru.yandex.practicum.collector.dto.hub.ScenarioCondition;
import ru.yandex.practicum.collector.dto.hub.ScenarioRemovedEvent;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;

import java.util.List;

@Component
public class HubEventMapper {

    public HubEventAvro toAvro(HubEvent event) {
        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(mapPayload(event))
                .build();
    }

    private Object mapPayload(HubEvent event) {
        return switch (event.getType()) {
            case DEVICE_ADDED -> {
                DeviceAddedEvent source = (DeviceAddedEvent) event;
                yield DeviceAddedEventAvro.newBuilder()
                        .setId(source.getId())
                        .setType(DeviceTypeAvro.valueOf(source.getDeviceType().name()))
                        .build();
            }
            case DEVICE_REMOVED -> {
                DeviceRemovedEvent source = (DeviceRemovedEvent) event;
                yield DeviceRemovedEventAvro.newBuilder()
                        .setId(source.getId())
                        .build();
            }
            case SCENARIO_ADDED -> {
                ScenarioAddedEvent source = (ScenarioAddedEvent) event;
                yield ScenarioAddedEventAvro.newBuilder()
                        .setName(source.getName())
                        .setConditions(mapConditions(source.getConditions()))
                        .setActions(mapActions(source.getActions()))
                        .build();
            }
            case SCENARIO_REMOVED -> {
                ScenarioRemovedEvent source = (ScenarioRemovedEvent) event;
                yield ScenarioRemovedEventAvro.newBuilder()
                        .setName(source.getName())
                        .build();
            }
        };
    }

    private List<ScenarioConditionAvro> mapConditions(List<ScenarioCondition> conditions) {
        return conditions.stream()
                .map(condition -> ScenarioConditionAvro.newBuilder()
                        .setSensorId(condition.getSensorId())
                        .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
                        .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()))
                        .setValue(condition.getValue())
                        .build())
                .toList();
    }

    private List<DeviceActionAvro> mapActions(List<DeviceAction> actions) {
        return actions.stream()
                .map(action -> DeviceActionAvro.newBuilder()
                        .setSensorId(action.getSensorId())
                        .setType(ActionTypeAvro.valueOf(action.getType().name()))
                        .setValue(action.getValue())
                        .build())
                .toList();
    }
}
