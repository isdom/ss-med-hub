package com.yulore.medhub.ws;

import com.yulore.util.ExceptionUtil;
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
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

@Configuration
@EnableConfigurationProperties(WsConfigProperties.class)
@RequiredArgsConstructor
@Slf4j
public class WsServerBuilder {
    @Bean(destroyMethod = "stop")
    public WebSocketServer webSocketServer() {
        log.info("webSocketServer: {}", configProps);
        final WebSocketServer server = new WebSocketServer(new InetSocketAddress(configProps.host, configProps.port)) {
            @Override
            public void onOpen(final WebSocket webSocket, final ClientHandshake handshake) {
                log.info("wscount/{}: new connection from {}, to: {}, uri path: {}",
                        _currentWSConnection.incrementAndGet(),
                        webSocket.getRemoteSocketAddress(),
                        webSocket.getLocalSocketAddress(),
                        handshake.getResourceDescriptor());

                handlerRegistry.createHandler(webSocket, handshake).ifPresentOrElse(handler -> {
                    // webSocket.setAttachment(handler);
                    // handler.onAttached(webSocket);
                    // handler.scheduleIdleCheck(executorProvider.getObject());
                }, () -> webSocket.close(1002, "Unsupported path"));
            }

            @Override
            public void onClose(final WebSocket webSocket, final int code, final String reason, final boolean remote) {
                final Object attachment = webSocket.getAttachment();
                if (attachment instanceof WsHandler handler) {
                    try {
                        handler.onClose(webSocket);
                    } catch (Exception ex) {
                        log.warn("handler.onClose: {} with exception {}",
                                webSocket.getRemoteSocketAddress(), ExceptionUtil.exception2detail(ex));
                    }
                }
                log.info("wscount/{}: closed {} with exit code {} additional info: {}",
                        _currentWSConnection.decrementAndGet(), webSocket.getRemoteSocketAddress(), code, reason);
            }

            @Override
            public void onMessage(final WebSocket webSocket, final String message) {
                log.info("received text message from {}: {}", webSocket.getRemoteSocketAddress(), message);
                final Object attachment = webSocket.getAttachment();
                if (attachment instanceof WsHandler handler) {
                    try {
                        handler.onMessage(webSocket, message);
                    } catch (Exception ex) {
                        log.warn("handler.onMessage: {}/{} with exception {}",
                                webSocket.getRemoteSocketAddress(), message, ExceptionUtil.exception2detail(ex));
                    }
                }
            }

            @Override
            public void onMessage(final WebSocket webSocket, final ByteBuffer bytes) {
                final Object attachment = webSocket.getAttachment();
                final int remaining = bytes.remaining();
                if (attachment instanceof WsHandler handler) {
                    try {
                        handler.onMessage(webSocket, bytes);
                    } catch (Exception ex) {
                        log.warn("handler.onMessage(Binary): {}/({} bytes) with exception {}",
                                webSocket.getRemoteSocketAddress(), remaining, ExceptionUtil.exception2detail(ex));
                    }
                } else {
                    log.error("onMessage(Binary): {} without any handler, ignore", webSocket.getRemoteSocketAddress());
                }
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

    private final WsHandlerRegistry handlerRegistry;
    private final WsConfigProperties configProps;
    private final ObjectProvider<ScheduledExecutorService> executorProvider;
    private final AtomicInteger _currentWSConnection = new AtomicInteger(0);
}
