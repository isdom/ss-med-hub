package com.yulore.medhub.ws;

import com.yulore.api.MasterService;
import com.yulore.util.ExceptionUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.redisson.api.RedissonClient;
import org.redisson.api.RemoteInvocationOptions;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.*;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

@Configuration
@EnableConfigurationProperties(WsConfigProperties.class)
@RequiredArgsConstructor
@Slf4j
public class WsServerBuilder {

    public static final String READ_RMS = "read_rms";

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
            public void onError(final WebSocket webSocket, final Exception ex) {
                log.warn("an error occurred on connection {}:{}",
                        webSocket.getRemoteSocketAddress(), ExceptionUtil.exception2detail(ex));

                final Object attachment = webSocket.getAttachment();
                if (attachment instanceof WsHandler handler) {
                    handler.onWebsocketError(ex);
                }
            }

            @Override
            public void onStart() {
            }
        };
        server.start();
        log.info("WS Server Started -- {}:{}", server.getAddress(), server.getPort());
        updateHubStatus();
        return server;
    }

    private void updateHubStatus() {
        final MasterService masterService = redisson.getRemoteService(_service_master)
                .get(MasterService.class, RemoteInvocationOptions.defaults()
                        .noAck()
                        .expectResultWithin(3, TimeUnit.SECONDS));

        final String ipAndPort = ipv4Provider.getObject().getHostAddress() + ":" + configProps.port;

        checkAndScheduleNext((startedTimestamp)-> {
            try {
                masterService.updateHubStatus(ipAndPort, configProps.pathMappings, System.currentTimeMillis());
                rrmsUrls.set(masterService.getUrlsOf(READ_RMS));
                log.debug("{}'s urls: {}", READ_RMS, rrmsUrls.get());
            } catch (Exception ex) {
                log.info("interact with masterService failed: {}", ex.toString());
            }
            return true;
        }, System.currentTimeMillis());
    }

    private void checkAndScheduleNext(final Function<Long, Boolean> doCheck, final long timestamp) {
        try {
            if (doCheck.apply(timestamp)) {
                schedulerProvider.getObject().schedule(()->checkAndScheduleNext(doCheck, timestamp), _check_interval, TimeUnit.MILLISECONDS);
            }
        } catch (final Exception ex) {
            log.warn("checkAndScheduleNext: exception: {}", ExceptionUtil.exception2detail(ex));
        }
    }

    @Bean
    public Inet4Address getRealIp() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                final NetworkInterface iface = interfaces.nextElement();
                // 过滤回环接口、虚拟接口和非活动接口
                if (iface.isLoopback() || iface.isVirtual() || !iface.isUp()) {
                    continue;
                }

                final Enumeration<InetAddress> addresses = iface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    final InetAddress addr = addresses.nextElement();
                    // 优先返回 IPv4 地址
                    if (addr instanceof Inet4Address ipv4) {
                        log.info("interface name: {}", iface.getDisplayName());
                        log.info("ipv4 address: {}", ipv4.getHostAddress());
                        return ipv4;
                    }
                }
            }
        } catch (SocketException ex) {
            log.warn("getRealIp: failed, detail: {}", ExceptionUtil.exception2detail(ex));
        }
        return null;
    }

    @Bean
    public HandlerUrlBuilder getUrlBuilder() {
        final AtomicInteger idx = new AtomicInteger(0);
        return handler -> {
            if (handler.equals(READ_RMS)) {
                final List<String> urls = rrmsUrls.get();
                final int size = urls.size();
                return size > 0 ? urls.get(idx.getAndIncrement() % size) : "";
            }
            return "";
        };
    }

    private final WsHandlerRegistry handlerRegistry;
    private final WsConfigProperties configProps;

    @Value("${service.master}")
    private String _service_master;

    @Value("${agent.check_interval:10000}") // default: 10 * 1000ms
    private long _check_interval;

    private final ObjectProvider<ScheduledExecutorService> schedulerProvider;
    private final ObjectProvider<Inet4Address> ipv4Provider;
    private final RedissonClient redisson;

    private final AtomicReference<List<String>> rrmsUrls = new AtomicReference<>(List.of());
    private final AtomicInteger _currentWSConnection = new AtomicInteger(0);
}
