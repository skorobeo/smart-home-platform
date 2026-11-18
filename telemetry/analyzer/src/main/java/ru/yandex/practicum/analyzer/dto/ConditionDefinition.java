package ru.yandex.practicum.analyzer.dto;

import ru.yandex.practicum.analyzer.model.ConditionOperation;
import ru.yandex.practicum.analyzer.model.ConditionType;

public record ConditionDefinition(
        String sensorId,
        ConditionType type,
        ConditionOperation operation,
        Integer value) {
}
