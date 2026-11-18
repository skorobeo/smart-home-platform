package ru.yandex.practicum.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.analyzer.dto.ActionDefinition;
import ru.yandex.practicum.analyzer.dto.ConditionDefinition;
import ru.yandex.practicum.analyzer.dto.ScenarioDefinition;
import ru.yandex.practicum.analyzer.model.Action;
import ru.yandex.practicum.analyzer.model.ActionType;
import ru.yandex.practicum.analyzer.model.Condition;
import ru.yandex.practicum.analyzer.model.ConditionOperation;
import ru.yandex.practicum.analyzer.model.ConditionType;
import ru.yandex.practicum.analyzer.model.Scenario;
import ru.yandex.practicum.analyzer.model.ScenarioAction;
import ru.yandex.practicum.analyzer.model.ScenarioCondition;
import ru.yandex.practicum.analyzer.model.Sensor;
import ru.yandex.practicum.analyzer.repository.ActionRepository;
import ru.yandex.practicum.analyzer.repository.ConditionRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioActionRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioConditionRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.analyzer.repository.SensorRepository;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ScenarioService {

    private final ScenarioRepository scenarioRepository;
    private final SensorRepository sensorRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;
    private final ScenarioActionRepository scenarioActionRepository;

    @Transactional
    public void addDevice(String hubId, String sensorId) {
        sensorRepository.findById(sensorId).ifPresentOrElse(
                existing -> {
                    if (!Objects.equals(existing.getHubId(), hubId)) {
                        log.warn("Датчик {} уже зарегистрирован в хабе {}, событие хаба {} проигнорировано",
                                sensorId, existing.getHubId(), hubId);
                    }
                },
                () -> sensorRepository.save(new Sensor(sensorId, hubId)));
    }

    @Transactional
    public void removeDevice(String hubId, String sensorId) {
        sensorRepository.findByIdAndHubId(sensorId, hubId).ifPresent(sensor -> {
            Set<Long> scenarioIds = new HashSet<>();
            scenarioConditionRepository.findByIdSensorId(sensorId)
                    .forEach(link -> scenarioIds.add(link.getId().getScenarioId()));
            scenarioActionRepository.findByIdSensorId(sensorId)
                    .forEach(link -> scenarioIds.add(link.getId().getScenarioId()));

            scenarioIds.forEach(this::deleteScenarioById);
            sensorRepository.delete(sensor);
            log.info("Датчик {} удалён из хаба {}", sensorId, hubId);
        });
    }

    @Transactional
    public void addScenario(String hubId, ScenarioAddedEventAvro event) {
        List<String> sensorIds = new ArrayList<>();
        event.getConditions().forEach(condition -> sensorIds.add(condition.getSensorId()));
        event.getActions().forEach(action -> sensorIds.add(action.getSensorId()));

        Set<String> uniqueSensorIds = new LinkedHashSet<>(sensorIds);
        for (String sensorId : uniqueSensorIds) {
            Optional<Sensor> existing = sensorRepository.findById(sensorId);
            if (existing.isEmpty()) {
                // Сценарий может прийти раньше события регистрации устройства.
                // Сохраняем датчик, чтобы не потерять пользовательский сценарий.
                sensorRepository.saveAndFlush(new Sensor(sensorId, hubId));
                log.info("Датчик {} создан по событию сценария '{}' хаба {}",
                        sensorId, event.getName(), hubId);
            } else if (!Objects.equals(existing.get().getHubId(), hubId)) {
                log.warn("Сценарий '{}' хаба {} пропущен: датчик {} принадлежит хабу {}",
                        event.getName(), hubId, sensorId, existing.get().getHubId());
                return;
            }
        }

        Scenario scenario = scenarioRepository.findByHubIdAndName(hubId, event.getName())
                .orElseGet(() -> new Scenario(hubId, event.getName()));
        if (scenario.getId() != null) {
            clearScenarioLinks(scenario.getId());
        }
        scenario = scenarioRepository.saveAndFlush(scenario);

        for (ScenarioConditionAvro conditionAvro : event.getConditions()) {
            Condition condition = conditionRepository.saveAndFlush(new Condition(
                    ConditionType.valueOf(conditionAvro.getType().name()),
                    ConditionOperation.valueOf(conditionAvro.getOperation().name()),
                    toInteger(conditionAvro.getValue())));
            scenarioConditionRepository.saveAndFlush(new ScenarioCondition(
                    scenario.getId(), conditionAvro.getSensorId(), condition.getId()));
        }

        for (DeviceActionAvro actionAvro : event.getActions()) {
            Action action = actionRepository.saveAndFlush(new Action(
                    ActionType.valueOf(actionAvro.getType().name()),
                    actionAvro.getValue() == null ? null : actionAvro.getValue()));
            scenarioActionRepository.saveAndFlush(new ScenarioAction(
                    scenario.getId(), actionAvro.getSensorId(), action.getId()));
        }

        log.info("Сценарий '{}' хаба {} сохранён", event.getName(), hubId);
    }

    @Transactional
    public void removeScenario(String hubId, String name) {
        scenarioRepository.findByHubIdAndName(hubId, name).ifPresent(scenario -> {
            clearScenarioLinks(scenario.getId());
            scenarioRepository.delete(scenario);
            log.info("Сценарий '{}' хаба {} удалён", name, hubId);
        });
    }

    @Transactional(readOnly = true)
    public List<ScenarioDefinition> findDefinitions(String hubId) {
        return scenarioRepository.findByHubId(hubId).stream()
                .map(this::toDefinition)
                .toList();
    }

    private ScenarioDefinition toDefinition(Scenario scenario) {
        List<ScenarioCondition> conditionLinks =
                scenarioConditionRepository.findByIdScenarioId(scenario.getId());
        List<Long> conditionIds = conditionLinks.stream()
                .map(link -> link.getId().getConditionId())
                .toList();
        Map<Long, Condition> conditions = conditionRepository.findAllById(conditionIds).stream()
                .collect(Collectors.toMap(Condition::getId, Function.identity()));

        List<ConditionDefinition> conditionDefinitions = conditionLinks.stream()
                .map(link -> {
                    Condition condition = conditions.get(link.getId().getConditionId());
                    if (condition == null) {
                        return null;
                    }
                    return new ConditionDefinition(
                            link.getId().getSensorId(),
                            condition.getType(),
                            condition.getOperation(),
                            condition.getValue());
                })
                .filter(Objects::nonNull)
                .toList();

        List<ScenarioAction> actionLinks = scenarioActionRepository.findByIdScenarioId(scenario.getId());
        List<Long> actionIds = actionLinks.stream()
                .map(link -> link.getId().getActionId())
                .toList();
        Map<Long, Action> actions = actionRepository.findAllById(actionIds).stream()
                .collect(Collectors.toMap(Action::getId, Function.identity()));

        List<ActionDefinition> actionDefinitions = actionLinks.stream()
                .map(link -> {
                    Action action = actions.get(link.getId().getActionId());
                    if (action == null) {
                        return null;
                    }
                    return new ActionDefinition(
                            link.getId().getSensorId(),
                            action.getType(),
                            action.getValue());
                })
                .filter(Objects::nonNull)
                .toList();

        return new ScenarioDefinition(scenario.getHubId(), scenario.getName(),
                conditionDefinitions, actionDefinitions);
    }

    private void deleteScenarioById(Long scenarioId) {
        scenarioRepository.findById(scenarioId).ifPresent(scenario -> {
            clearScenarioLinks(scenarioId);
            scenarioRepository.delete(scenario);
        });
    }

    private void clearScenarioLinks(Long scenarioId) {
        List<Long> conditionIds = scenarioConditionRepository.findByIdScenarioId(scenarioId).stream()
                .map(link -> link.getId().getConditionId())
                .toList();
        List<Long> actionIds = scenarioActionRepository.findByIdScenarioId(scenarioId).stream()
                .map(link -> link.getId().getActionId())
                .toList();

        scenarioConditionRepository.deleteByScenarioId(scenarioId);
        scenarioActionRepository.deleteByScenarioId(scenarioId);
        scenarioConditionRepository.flush();
        scenarioActionRepository.flush();

        if (!conditionIds.isEmpty()) {
            conditionRepository.deleteAllById(conditionIds);
        }
        if (!actionIds.isEmpty()) {
            actionRepository.deleteAllById(actionIds);
        }
    }

    private Integer toInteger(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean bool) {
            return bool ? 1 : 0;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        return null;
    }
}
