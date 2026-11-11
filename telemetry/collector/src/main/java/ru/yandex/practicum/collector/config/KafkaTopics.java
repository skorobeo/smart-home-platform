package ru.yandex.practicum.collector.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "collector.kafka.topics")
public class KafkaTopics {

    private String sensors;
    private String hubs;
}
