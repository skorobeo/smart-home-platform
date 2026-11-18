package ru.yandex.practicum.analyzer.dto;

import ru.yandex.practicum.analyzer.model.ActionType;

public record ActionDefinition(String sensorId, ActionType type, Integer value) {
}
