package ru.yandex.practicum.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.config.KafkaTopics;
import ru.yandex.practicum.collector.dto.sensor.SensorEvent;
import ru.yandex.practicum.collector.mapper.SensorEventMapper;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Slf4j
@Service
@RequiredArgsConstructor
public class SensorEventService {

    private final KafkaTemplate<String, SpecificRecord> kafkaTemplate;
    private final SensorEventMapper sensorEventMapper;
    private final KafkaTopics kafkaTopics;

    public void collectSensorEvent(SensorEvent event) {
        SensorEventAvro avro = sensorEventMapper.toAvro(event);
        String topic = kafkaTopics.getSensors();
        kafkaTemplate.send(topic, event.getId(), avro)
                .whenComplete((result, exception) -> {
                    if (exception != null) {
                        log.error("Не удалось отправить событие датчика {} в топик {}", event.getId(), topic, exception);
                    } else {
                        log.info("Событие датчика {} отправлено в топик {}", event.getId(), topic);
                    }
                });
    }
}
