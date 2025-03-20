package com.yulore.medhub.ws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yulore.metric.MetricCustomized;
import com.yulore.util.ExceptionUtil;
import com.yulore.util.NetworkUtil;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
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
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

@Configuration
@EnableConfigurationProperties(WsConfigProperties.class)
@RequiredArgsConstructor
@Slf4j
public class WsServerBuilder {

    public static final String READ_RMS = "read_rms";

    @Bean(destroyMethod = "stop")
    public WebSocketServer webSocketServer() {
        log.info("webSocketServer: {}", configProps);
        final Timer timer = timerProvider.getObject("mh.connected.delay", null);
        gaugeProvider.getObject((Supplier<Number>)_currentWSConnection::get, "mh.ws.count",
                MetricCustomized.builder().tags(List.of("actor", "all")).build());

        final WebSocketServer server = new WebSocketServer(new InetSocketAddress(configProps.host, configProps.port)) {
            @Override
            public void onOpen(final WebSocket webSocket, final ClientHandshake handshake) {
                log.info("wscount/{}: new connection from {}, to: {}, uri path: {}",
                        _currentWSConnection.incrementAndGet(),
                        webSocket.getRemoteSocketAddress(),
                        webSocket.getLocalSocketAddress(),
                        handshake.getResourceDescriptor());

                final String str = handshake.getFieldValue("x-timestamp");
                if (str != null && !str.isEmpty()) {
                    final long startConnectInMs = Long.parseLong(str) / 1000; // nanosecond to millisecond
                    final long delay = System.currentTimeMillis() - startConnectInMs;
                    final StringBuilder sb = new StringBuilder();
                    handshake.iterateHttpFields().forEachRemaining(s -> {
                        if (s.startsWith("x-")) {
                            sb.append(handshake.getFieldValue(s)).append(":");
                        }
                    });
                    log.info("ws: {}/{} connected delay: {} ms", handshake.getResourceDescriptor(), sb, delay);
                    timer.record(delay, TimeUnit.MILLISECONDS);
                }
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
                        log.warn("handler.onClose: {} failed", webSocket.getRemoteSocketAddress(), ex);
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
        launchUpdateHubStatus();
        return server;
    }

    private void launchUpdateHubStatus() {
        final MasterServiceAsync masterService = redisson.getRemoteService(_service_master)
                .get(MasterServiceAsync.class, RemoteInvocationOptions.defaults()
                        .noAck()
                        .expectResultWithin(3, TimeUnit.SECONDS));

        final String ipAndPort = ipv4Provider.getObject().getHostAddress() + ":" + configProps.port;
        final Timer timer1 = timerProvider.getObject("redisson.rs.duration",
                MetricCustomized.builder().tags(List.of("method", "updateHubStatus")).build());
        final Timer timer2 = timerProvider.getObject("redisson.rs.duration",
                MetricCustomized.builder().tags(List.of("method", "getUrlsOf")).build());

        checkAndScheduleNext(startedTimestamp -> {
            final AtomicReference<Timer.Sample> sampleRef = new AtomicReference<>(Timer.start());
            return masterService.updateHubStatus(System.currentTimeMillis(), ipAndPort, configProps.pathMappings, buildInfo())
                    .thenCompose(v -> {
                        sampleRef.get().stop(timer1);
                        sampleRef.set(Timer.start());
                        return masterService.getUrlsOf(READ_RMS);
                    })
                    .handle((urls, ex) -> {
                        if (ex != null) {
                            log.warn("interact with masterService failed: {}", ex.toString());
                        } else {
                            sampleRef.get().stop(timer2);
                            rrmsUrls.set(urls);
                            log.info("{}'s urls: {}", READ_RMS, rrmsUrls.get());
                        }
                        return true;
                    });
        }, System.currentTimeMillis());
    }

    private String buildInfo() {
        try {
            return new ObjectMapper().writeValueAsString(HubInfo.builder().wscount(_currentWSConnection.get()).build());
        } catch (JsonProcessingException ex) {
            log.warn("buildInfo failed", ex);
            return null;
        }
    }

    private void checkAndScheduleNext(final Function<Long, CompletionStage<Boolean>> doCheck, final long timestamp) {
        doCheck.apply(timestamp).whenComplete((isContinue, ex)->{
            if (ex != null) {
                log.warn("checkAndScheduleNext: failed", ex);
                schedulerProvider.getObject().schedule(()->checkAndScheduleNext(doCheck, timestamp), _check_interval, TimeUnit.MILLISECONDS);
            } else {
                if (isContinue) {
                    schedulerProvider.getObject().schedule(()->checkAndScheduleNext(doCheck, timestamp), _check_interval, TimeUnit.MILLISECONDS);
                } else {
                    log.warn("checkAndScheduleNext: isContinue is false, stop schedule next");
                }
            }
        });
    }

    @Bean
    public Inet4Address getRealIp() {
        return NetworkUtil.getLocalIpv4();
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
    private final ObjectProvider<Timer> timerProvider;
    private final ObjectProvider<Gauge> gaugeProvider;

    private final AtomicReference<List<String>> rrmsUrls = new AtomicReference<>(List.of());
    private final AtomicInteger _currentWSConnection = new AtomicInteger(0);
}
