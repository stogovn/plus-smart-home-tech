package ru.yandex.practicum.handler.hub;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.DeviceAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.time.Instant;

@Component
public class DeviceAddedEventHandler extends AbstractHubEventHandler {

    public DeviceAddedEventHandler(KafkaTemplate<String, Object> hubKafkaTemplate) {
        super(hubKafkaTemplate);
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_ADDED;
    }

    @Override
    protected HubEventAvro mapToAvro(HubEventProto eventProto) {
        DeviceAddedEventProto deviceAddedProto = eventProto.getDeviceAdded();
        DeviceAddedEventAvro deviceAddedAvro = DeviceAddedEventAvro.newBuilder()
                .setId(deviceAddedProto.getId())
                .setType(DeviceTypeAvro.valueOf(deviceAddedProto.getType().name()))
                .build();
        return HubEventAvro.newBuilder()
                .setHubId(eventProto.getHubId())
                .setTimestamp(Instant.ofEpochSecond(eventProto.getTimestamp().getSeconds(),
                        eventProto.getTimestamp().getNanos()))
                .setPayload(deviceAddedAvro)
                .build();
    }
}
