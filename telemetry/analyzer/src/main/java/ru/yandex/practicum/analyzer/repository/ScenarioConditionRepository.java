package ru.yandex.practicum.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.yandex.practicum.analyzer.model.ScenarioCondition;
import ru.yandex.practicum.analyzer.model.ScenarioConditionId;

import java.util.List;

public interface ScenarioConditionRepository extends JpaRepository<ScenarioCondition, ScenarioConditionId> {

    List<ScenarioCondition> findByIdScenarioId(Long scenarioId);

    List<ScenarioCondition> findByIdSensorId(String sensorId);

    @Modifying
    @Query("delete from ScenarioCondition sc where sc.id.scenarioId = :scenarioId")
    void deleteByScenarioId(@Param("scenarioId") Long scenarioId);

    @Modifying
    @Query("delete from ScenarioCondition sc where sc.id.sensorId = :sensorId")
    void deleteBySensorId(@Param("sensorId") String sensorId);
}
