package com.yulore.medhub.ws;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Configuration
@EnableConfigurationProperties(WsConfigProperties.class)
@RequiredArgsConstructor
@Slf4j
public class WsServerBuilder {
    private final WsHandlerRegistry handlerRegistry;
    private final WsConfigProperties configProps;
    private final ObjectProvider<ScheduledExecutorService> executorProvider;

    @Bean(destroyMethod = "stop")
    public WebSocketServer webSocketServer() throws Exception {
        final WebSocketServer server = new WebSocketServer(new InetSocketAddress(configProps.host, configProps.port)) {
            @Override
            public void onOpen(final WebSocket webSocket, final ClientHandshake handshake) {
                handlerRegistry.createHandler(webSocket, handshake).ifPresentOrElse(handler -> {
                    webSocket.setAttachment(handler);
                    handler.onAttached(webSocket);
                    // handler.scheduleIdleCheck(executorProvider.getObject());
                }, () -> webSocket.close(1002, "Unsupported path"));
            }

            @Override
            public void onClose(WebSocket webSocket, int i, String s, boolean b) {
            }

            @Override
            public void onMessage(WebSocket webSocket, String s) {
            }

            @Override
            public void onError(WebSocket webSocket, Exception e) {
            }

            @Override
            public void onStart() {
            }
        };
        server.start();
        log.info("WS Server Started -- {}:{}", server.getAddress(), server.getPort());
        return server;
    }

    @Bean
    public ScheduledExecutorService scheduledExecutor() {
        return Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors() * 2);
    }
}
