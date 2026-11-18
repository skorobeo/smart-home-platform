package ru.yandex.practicum.analyzer.dto;

import java.util.List;

public record ScenarioDefinition(
        String hubId,
        String name,
        List<ConditionDefinition> conditions,
        List<ActionDefinition> actions) {
}
