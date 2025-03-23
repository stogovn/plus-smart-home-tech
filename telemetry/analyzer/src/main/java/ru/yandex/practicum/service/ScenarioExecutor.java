package ru.yandex.practicum.service;

import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;

@Service
public class ScenarioExecutor {
    private final HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    public ScenarioExecutor(@GrpcClient("hub-router")
                            HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient) {
        this.hubRouterClient = hubRouterClient;
    }

    public void send(DeviceActionRequest request) {
        hubRouterClient.handleDeviceAction(request);
    }
}
