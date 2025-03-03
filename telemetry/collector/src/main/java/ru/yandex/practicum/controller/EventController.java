package ru.yandex.practicum.controller;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

@Slf4j
@GrpcService
public class EventController extends CollectorControllerGrpc.CollectorControllerImplBase {

    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.info("Получено событие от датчика: {}", request);

            SensorEventProto.PayloadCase payloadCase = request.getPayloadCase();
            switch (payloadCase) {
                case LIGHT_SENSOR_EVENT:
                    log.info("Получено событие датчика освещённости");
                    log.info("Уровень освещённости: {}", request.getLightSensorEvent().getLuminosity());
                    break;
                case CLIMATE_SENSOR_EVENT:
                    log.info("Получено событие климатического датчика");
                    log.info("Влажность: {}", request.getClimateSensorEvent().getHumidity());
                    break;
                case MOTION_SENSOR_EVENT:
                    log.info("Получено событие датчика движения");
                    log.info("Наличие движения: {}", request.getMotionSensorEvent().getMotion());
                    break;
                case SWITCH_SENSOR_EVENT:
                    log.info("Получено событие датчика-переключателя");
                    log.info("Состояние: {}", request.getSwitchSensorEvent().getState());
                    break;
                case TEMPERATURE_SENSOR_EVENT:
                    log.info("Получено событие температурного датчика");
                    log.info("Температура по Цельсию: {}", request.getTemperatureSensorEvent().getTemperatureC());
                    break;
                default:
                    log.warn("Получено событие sensor неизвестного типа: {}", payloadCase);
                    break;
            }

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка при обработке события SensorEventProto: ", e);
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL.withDescription(e.getLocalizedMessage()).withCause(e)
            ));
        }
    }

    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.info("Получено событие от хаба: {}", request);

            HubEventProto.PayloadCase payloadCase = request.getPayloadCase();
            switch (payloadCase) {
                case DEVICE_ADDED:
                    log.info("Получено событие добавления устройства");
                    log.info("Идентификатор добавляемого устройства: {}", request.getDeviceAdded().getId());
                    break;
                case DEVICE_REMOVED:
                    log.info("Получено событие удаления устройства");
                    log.info("Идентификатор удаляемого устройства: {}", request.getDeviceRemoved().getId());
                    break;
                case SCENARIO_ADDED:
                    log.info("Получено событие добавления сценария");
                    break;
                case SCENARIO_REMOVED:
                    log.info("Получено событие удаления сценария");
                    break;
                default:
                    log.warn("Получено событие hub неизвестного типа: {}", payloadCase);
                    break;
            }

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка при обработке события HubEventProto: ", e);
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL.withDescription(e.getLocalizedMessage()).withCause(e)
            ));
        }
    }
}
