package ru.yandex.practicum.analyzer.model;

import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "scenario_actions")
@Getter
@Setter
@NoArgsConstructor
public class ScenarioAction {

    @EmbeddedId
    private ScenarioActionId id;

    public ScenarioAction(Long scenarioId, String sensorId, Long actionId) {
        this.id = new ScenarioActionId(scenarioId, sensorId, actionId);
    }
}
