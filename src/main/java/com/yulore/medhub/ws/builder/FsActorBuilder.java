package com.yulore.medhub.ws.builder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yulore.medhub.api.ScriptApi;
import com.yulore.medhub.service.ASRService;
import com.yulore.medhub.vo.WSCommandVO;
import com.yulore.medhub.ws.actor.FsActor;
import com.yulore.medhub.vo.WSEventVO;
import com.yulore.medhub.ws.WsHandler;
import com.yulore.medhub.ws.WsHandlerBuilder;
import com.yulore.util.ExceptionUtil;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Component("fsHandler")
@RequiredArgsConstructor
@Slf4j
public class FsActorBuilder implements WsHandlerBuilder {
    @PostConstruct
    public void start() {
        _sessionExecutor = Executors.newFixedThreadPool(NettyRuntime.availableProcessors() * 2,
                new DefaultThreadFactory("sessionExecutor"));
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        _sessionExecutor.shutdownNow();
    }

    @Override
    public WsHandler build(final String prefix, final WebSocket webSocket, final ClientHandshake handshake) {
        // init FsActor attach with webSocket
        final String role = handshake.getFieldValue("x-role");
        if ("asr".equals(role)) {
            final String uuid = handshake.getFieldValue("x-uuid");
            final String sessionId = handshake.getFieldValue("x-sessionid");
            final String welcome = handshake.getFieldValue("x-welcome");
            final String recordStartTimestamp = handshake.getFieldValue("x-rst");

            final FsActor actor = new FsActor(
                    uuid,
                    sessionId,
                    _scriptApi,
                    welcome,
                    recordStartTimestamp,
                    _rms_cp_prefix,
                    _rms_tts_prefix,
                    _rms_wav_prefix,
                    _test_enable_delay,
                    _test_delay_ms,
                    _test_enable_disconnect,
                    _test_disconnect_probability,
                    ()->webSocket.close(1006, "test_disconnect")) {
                @Override
                public void onMessage(final WebSocket webSocket, final String message) {
                    try {
                        handleCommand(new ObjectMapper().readValue(message, WSCommandVO.class), webSocket);
                    } catch (JsonProcessingException ex) {
                        log.error("handleHubCommand {}: {}, an error occurred when parseAsJson: {}",
                                webSocket.getRemoteSocketAddress(), message, ExceptionUtil.exception2detail(ex));
                    }
                }

                @Override
                public void onMessage(final WebSocket webSocket, final ByteBuffer bytes) {
                    if (transmit(bytes)) {
                        // transmit success
                        if ((transmitCount() % 50) == 0) {
                            log.debug("{}: transmit 50 times.", sessionId());
                        }
                    }
                }
            };

            webSocket.setAttachment(actor);
            actor.scheduleCheckIdle(schedulerProvider.getObject(), _check_idle_interval_ms, actor::checkIdle);
            WSEventVO.<Void>sendEvent(webSocket, "FSConnected", null);
            log.info("ws path match {}, role: {}. using ws as FsActor {}", prefix, role, sessionId);
            return actor;
        } else {
            final String sessionId = handshake.getFieldValue("x-sessionid");
            log.info("onOpen: sessionid: {} for ws: {}", sessionId, webSocket.getRemoteSocketAddress());
            final FsActor actor = FsActor.findBy(sessionId);
            if (actor != null) {
                webSocket.setAttachment(actor);
                actor.attachPlaybackWs((event, payload) -> {
                    try {
                        WSEventVO.sendEvent(webSocket, event, payload);
                    } catch (Exception ex) {
                        log.warn("[{}]: FsActor sendback {}/{} failed, detail: {}", actor.sessionId(), event, payload,
                                ExceptionUtil.exception2detail(ex));
                    }
                });
                log.info("ws path match: {}, role: {}, attach exist FsActor {}", prefix, role, sessionId);
            } else {
                log.warn("ws path match: {}, role: {}, !NOT! find FsActor with {}", prefix, role, sessionId);
            }
            return actor;
        }
    }

    private void handleCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        if ("StartTranscription".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> asrService.startTranscription(cmd, webSocket));
        } else if ("StopTranscription".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> asrService.stopTranscription(cmd, webSocket));
        } else if ("FSPlaybackStarted".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleFSPlaybackStartedCommand(cmd, webSocket));
        } else if ("FSPlaybackStopped".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleFSPlaybackStoppedCommand(cmd, webSocket));
        } else if ("FSPlaybackPaused".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleFSPlaybackPausedCommand(cmd, webSocket));
        } else if ("FSPlaybackResumed".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleFSPlaybackResumedCommand(cmd, webSocket));
        } else if ("FSRecordStarted".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleFSRecordStartedCommand(cmd, webSocket));
        }  else {
            log.warn("handleCommand: Unknown Command: {}", cmd);
        }
    }

    private void handleFSRecordStartedCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        if (webSocket.getAttachment() instanceof FsActor session) {
            session.notifyFSRecordStarted(cmd);
        }
    }

    private void handleFSPlaybackStartedCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        if (webSocket.getAttachment() instanceof FsActor session) {
            session.notifyFSPlaybackStarted(cmd);
        }
    }

    private void handleFSPlaybackStoppedCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        if (webSocket.getAttachment() instanceof FsActor session) {
            session.notifyFSPlaybackStopped(cmd);
        }
    }

    private void handleFSPlaybackResumedCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        if (webSocket.getAttachment() instanceof FsActor fsActor) {
            fsActor.notifyPlaybackResumed(cmd);
        }
    }

    private void handleFSPlaybackPausedCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        if (webSocket.getAttachment() instanceof FsActor fsActor) {
            fsActor.notifyPlaybackPaused(cmd);
        }
    }

    @Resource
    private ScriptApi _scriptApi;

    @Value("${rms.cp_prefix}")
    private String _rms_cp_prefix;

    @Value("${rms.tts_prefix}")
    private String _rms_tts_prefix;

    @Value("${rms.wav_prefix}")
    private String _rms_wav_prefix;

    @Value("${test.enable_delay}")
    private boolean _test_enable_delay;

    @Value("${test.delay_ms}")
    private long _test_delay_ms;

    @Value("${test.enable_disconnect}")
    private boolean _test_enable_disconnect;

    @Value("${test.disconnect_probability}")
    private float _test_disconnect_probability;

    @Value("${session.check_idle_interval_ms}")
    private long _check_idle_interval_ms;

    private final ObjectProvider<ScheduledExecutorService> schedulerProvider;

    private ExecutorService _sessionExecutor;

    @Autowired
    private ASRService asrService;
}
