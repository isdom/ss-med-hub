package com.yulore.medhub.ws.builder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.yulore.medhub.api.ScriptApi;
import com.yulore.medhub.service.ASRService;
import com.yulore.medhub.service.CommandExecutor;
import com.yulore.medhub.vo.WSCommandVO;
import com.yulore.medhub.vo.cmd.*;
import com.yulore.medhub.ws.HandlerUrlBuilder;
import com.yulore.medhub.ws.WSCommandRegistry;
import com.yulore.medhub.ws.actor.FsActor;
import com.yulore.medhub.vo.WSEventVO;
import com.yulore.medhub.ws.WsHandler;
import com.yulore.medhub.ws.WsHandlerBuilder;
import com.yulore.util.ExceptionUtil;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.nio.ByteBuffer;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
@RequiredArgsConstructor
@Component("fs_io")
@ConditionalOnProperty(prefix = "feature", name = "fs_io", havingValue = "enabled")
public class FsActorBuilder implements WsHandlerBuilder {
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
                    urlProvider.getObject(),
                    _test_enable_delay,
                    _test_delay_ms,
                    _test_enable_disconnect,
                    _test_disconnect_probability,
                    ()->webSocket.close(1006, "test_disconnect")) {
                @Override
                public void onMessage(final WebSocket webSocket, final String message) {
                    final Timer.Sample sample = Timer.start();
                    cmdExecutorProvider.getObject().submit(()->{
                        try {
                            cmds.handleCommand(WSCommandVO.parse(message, WSCommandVO.WSCMD_VOID), message, this, webSocket, sample);
                        } catch (JsonProcessingException ex) {
                            log.error("handleCommand {}: {}, an error occurred when parseAsJson: {}",
                                    webSocket.getRemoteSocketAddress(), message, ExceptionUtil.exception2detail(ex));
                        }
                    });
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
    private final ObjectProvider<HandlerUrlBuilder> urlProvider;
    private final ObjectProvider<CommandExecutor> cmdExecutorProvider;

    @Autowired
    private ASRService asrService;

    final WSCommandRegistry<FsActor> cmds = new WSCommandRegistry<FsActor>()
            .register(VOStartTranscription.TYPE,"StartTranscription",
    ctx-> asrService.startTranscription(ctx.payload(), ctx.ws()))
            .register(WSCommandVO.WSCMD_VOID,"StopTranscription",
                      ctx-> asrService.stopTranscription(ctx.ws()))
            .register(VOFSPlaybackStarted.TYPE,"FSPlaybackStarted",
                      ctx->ctx.actor().notifyFSPlaybackStarted(ctx.payload()))
            .register(VOFSPlaybackStopped.TYPE,"FSPlaybackStopped",
                      ctx->ctx.actor().notifyFSPlaybackStopped(ctx.payload()))
            .register(VOFSPlaybackPaused.TYPE,"FSPlaybackPaused",
                      ctx->ctx.actor().notifyPlaybackPaused(ctx.payload()))
            .register(VOFSPlaybackResumed.TYPE,"FSPlaybackResumed",
                      ctx->ctx.actor().notifyPlaybackResumed(ctx.payload()))
            .register(VOFSRecordStarted.TYPE,"FSRecordStarted",
                      ctx->ctx.actor().notifyFSRecordStarted(ctx.payload()))
            ;
}
