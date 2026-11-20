package ru.yandex.practicum.analyzer.service;

import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;

@Slf4j
@Service
public class DeviceActionSender {

    private final HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    public DeviceActionSender(@GrpcClient("hub-router")
                              HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient) {
        this.hubRouterClient = hubRouterClient;
    }

    public void send(DeviceActionRequest request) {
        hubRouterClient.handleDeviceAction(request);
        log.info("Команда сценария '{}' для хаба {} отправлена: датчик {}, действие {}",
                request.getScenarioName(), request.getHubId(),
                request.getAction().getSensorId(), request.getAction().getType());
    }
}
