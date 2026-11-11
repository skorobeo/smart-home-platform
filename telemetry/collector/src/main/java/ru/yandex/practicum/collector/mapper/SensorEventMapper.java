package ru.yandex.practicum.collector.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.dto.sensor.ClimateSensorEvent;
import ru.yandex.practicum.collector.dto.sensor.LightSensorEvent;
import ru.yandex.practicum.collector.dto.sensor.MotionSensorEvent;
import ru.yandex.practicum.collector.dto.sensor.SensorEvent;
import ru.yandex.practicum.collector.dto.sensor.SwitchSensorEvent;
import ru.yandex.practicum.collector.dto.sensor.TemperatureSensorEvent;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;

@Component
public class SensorEventMapper {

    public SensorEventAvro toAvro(SensorEvent event) {
        return SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(mapPayload(event))
                .build();
    }

    private Object mapPayload(SensorEvent event) {
        return switch (event.getType()) {
            case LIGHT_SENSOR_EVENT -> {
                LightSensorEvent source = (LightSensorEvent) event;
                yield LightSensorAvro.newBuilder()
                        .setLinkQuality(source.getLinkQuality())
                        .setLuminosity(source.getLuminosity())
                        .build();
            }
            case SWITCH_SENSOR_EVENT -> {
                SwitchSensorEvent source = (SwitchSensorEvent) event;
                yield SwitchSensorAvro.newBuilder()
                        .setState(source.isState())
                        .build();
            }
            case CLIMATE_SENSOR_EVENT -> {
                ClimateSensorEvent source = (ClimateSensorEvent) event;
                yield ClimateSensorAvro.newBuilder()
                        .setTemperatureC(source.getTemperatureC())
                        .setHumidity(source.getHumidity())
                        .setCo2Level(source.getCo2Level())
                        .build();
            }
            case MOTION_SENSOR_EVENT -> {
                MotionSensorEvent source = (MotionSensorEvent) event;
                yield MotionSensorAvro.newBuilder()
                        .setLinkQuality(source.getLinkQuality())
                        .setMotion(source.isMotion())
                        .setVoltage(source.getVoltage())
                        .build();
            }
            case TEMPERATURE_SENSOR_EVENT -> {
                TemperatureSensorEvent source = (TemperatureSensorEvent) event;
                yield TemperatureSensorAvro.newBuilder()
                        .setId(source.getId())
                        .setHubId(source.getHubId())
                        .setTimestamp(source.getTimestamp())
                        .setTemperatureC(source.getTemperatureC())
                        .setTemperatureF(source.getTemperatureF())
                        .build();
            }
        };
    }
}
