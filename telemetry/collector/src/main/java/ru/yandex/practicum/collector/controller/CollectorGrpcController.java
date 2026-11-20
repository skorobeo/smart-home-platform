package ru.yandex.practicum.collector.controller;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.yandex.practicum.collector.mapper.ProtoEventMapper;
import ru.yandex.practicum.collector.service.HubEventService;
import ru.yandex.practicum.collector.service.SensorEventService;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@GrpcService
@RequiredArgsConstructor
public class CollectorGrpcController extends CollectorControllerGrpc.CollectorControllerImplBase {

    private static final long SEND_TIMEOUT_SECONDS = 10;

    private final SensorEventService sensorEventService;
    private final HubEventService hubEventService;
    private final ProtoEventMapper protoEventMapper;

    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            sensorEventService.sendSensorEvent(protoEventMapper.toDto(request))
                    .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Отправка события датчика {} прервана", request.getId(), e);
            responseObserver.onError(Status.ABORTED
                    .withDescription("Отправка события датчика прервана")
                    .withCause(e)
                    .asRuntimeException());
        } catch (ExecutionException e) {
            log.error("Не удалось отправить событие датчика {}", request.getId(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Не удалось сохранить событие датчика в Kafka")
                    .withCause(e.getCause())
                    .asRuntimeException());
        } catch (TimeoutException e) {
            log.error("Истекло время ожидания записи события датчика {}", request.getId(), e);
            responseObserver.onError(Status.DEADLINE_EXCEEDED
                    .withDescription("Истекло время ожидания записи события датчика в Kafka")
                    .withCause(e)
                    .asRuntimeException());
        } catch (Exception e) {
            log.error("Не удалось обработать событие датчика {}", request.getId(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            hubEventService.sendHubEvent(protoEventMapper.toDto(request))
                    .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Отправка события хаба {} прервана", request.getHubId(), e);
            responseObserver.onError(Status.ABORTED
                    .withDescription("Отправка события хаба прервана")
                    .withCause(e)
                    .asRuntimeException());
        } catch (ExecutionException e) {
            log.error("Не удалось отправить событие хаба {}", request.getHubId(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Не удалось сохранить событие хаба в Kafka")
                    .withCause(e.getCause())
                    .asRuntimeException());
        } catch (TimeoutException e) {
            log.error("Истекло время ожидания записи события хаба {}", request.getHubId(), e);
            responseObserver.onError(Status.DEADLINE_EXCEEDED
                    .withDescription("Истекло время ожидания записи события хаба в Kafka")
                    .withCause(e)
                    .asRuntimeException());
        } catch (Exception e) {
            log.error("Не удалось обработать событие хаба {}", request.getHubId(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }
}
