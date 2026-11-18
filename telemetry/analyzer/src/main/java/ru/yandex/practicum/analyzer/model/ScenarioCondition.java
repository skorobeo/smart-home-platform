package ru.yandex.practicum.analyzer.model;

import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "scenario_conditions")
@Getter
@Setter
@NoArgsConstructor
public class ScenarioCondition {

    @EmbeddedId
    private ScenarioConditionId id;

    public ScenarioCondition(Long scenarioId, String sensorId, Long conditionId) {
        this.id = new ScenarioConditionId(scenarioId, sensorId, conditionId);
    }
}
