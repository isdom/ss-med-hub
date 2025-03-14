package com.yulore.medhub.ws.builder;

import com.aliyun.oss.OSS;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yulore.bst.*;
import com.yulore.medhub.api.CallApi;
import com.yulore.medhub.api.ScriptApi;
import com.yulore.medhub.service.ASRService;
import com.yulore.medhub.service.BSTService;
import com.yulore.medhub.service.CommandExecutor;
import com.yulore.medhub.task.PlayStreamPCMTask2;
import com.yulore.medhub.task.SampleInfo;
import com.yulore.medhub.vo.*;
import com.yulore.medhub.vo.cmd.*;
import com.yulore.medhub.ws.WsHandler;
import com.yulore.medhub.ws.WsHandlerBuilder;
import com.yulore.medhub.ws.actor.PoActor;
import com.yulore.util.ExceptionUtil;
import com.yulore.util.VarsUtil;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@RequiredArgsConstructor
@Component("po_io")
@ConditionalOnProperty(prefix = "feature", name = "po_io", havingValue = "enabled")
public class PoActorBuilder implements WsHandlerBuilder {
    @PostConstruct
    public void start() {
        _ossAccessExecutor = Executors.newFixedThreadPool(NettyRuntime.availableProcessors() * 2,
                new DefaultThreadFactory("ossAccessExecutor"));
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        _ossAccessExecutor.shutdownNow();
    }

    // wss://domain/path?uuid=XX&tid=XXX&role=call
    // wss://domain/path?sessionId=xxx&role=playback
    @Override
    public WsHandler build(final String prefix, final WebSocket webSocket, final ClientHandshake handshake) {
        final String path = handshake.getResourceDescriptor();
        final int varsBegin = path.indexOf('?');
        final String role = varsBegin > 0 ? VarsUtil.extractValueWithSplitter(path.substring(varsBegin + 1), "role", '&') : null;

        if ("call".equals(role)) {
            // means ws with role: call
            // init PoActor attach with webSocket
            final String uuid = VarsUtil.extractValueWithSplitter(path.substring(varsBegin + 1), "uuid", '&');
            final String tid = VarsUtil.extractValueWithSplitter(path.substring(varsBegin + 1), "tid", '&');
            final String clientIp = handshake.getFieldValue("X-Forwarded-For");
            final PoActor actor = new PoActor(
                    clientIp,
                    uuid,
                    tid,
                    _callApi,
                    _scriptApi,
                    (_session) -> {
                        try {
                            WSEventVO.sendEvent(webSocket, "CallEnded", new PayloadCallEnded(_session.sessionId()));
                            log.info("[{}]: sendback CallEnded event", _session.sessionId());
                        } catch (Exception ex) {
                            log.warn("[{}]: sendback CallEnded event failed, detail: {}", _session.sessionId(), ex.toString());
                        }
                    },
                    _oss_bucket,
                    _oss_path,
                    (ctx) -> {
                        final long startUploadInMs = System.currentTimeMillis();
                        _ossAccessExecutor.submit(() -> {
                            _ossProvider.getObject().putObject(ctx.bucketName(), ctx.objectName(), ctx.content());
                            log.info("[{}]: upload record to oss => bucket:{}/object:{}, cost {} ms",
                                    ctx.sessionId(), ctx.bucketName(), ctx.objectName(), System.currentTimeMillis() - startUploadInMs);
                        });
                    },
                    (_sessionId) -> WSEventVO.sendEvent(webSocket, "CallStarted", new PayloadCallStarted(_sessionId))) {
                @Override
                public void onMessage(final WebSocket webSocket, final String message) {
                    cmdExecutorProvider.getObject().submit(()-> {
                        try {
                            handleCommand(WSCommandVO.parse(message, WSCommandVO.WSCMD_VOID), message, webSocket, this);
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
                            log.info("{}: transmit 50 times.", sessionId());
                        }
                    }
                }
            };
            webSocket.setAttachment(actor);
            actor.onAttached(webSocket);

            actor.scheduleCheckIdle(schedulerProvider.getObject(), _check_idle_interval_ms, actor::checkIdle);
            schedulerProvider.getObject().schedule(actor::notifyMockAnswer, _answer_timeout_ms, TimeUnit.MILLISECONDS);

            log.info("ws path match: {}, using ws as PoActor with role: {}", prefix, role);
            return actor;
        } else {
            final String sessionId = varsBegin > 0 ? VarsUtil.extractValueWithSplitter(path.substring(varsBegin + 1), "sessionId", '&') : null;
            // init PlaybackSession attach with webSocket
            log.info("ws path match: {}, role: {}, using ws as PoActor's playback ws: [{}]", prefix, role, sessionId);
            final PoActor actor = PoActor.findBy(sessionId);
            if (actor == null) {
                log.info("can't find callSession by sessionId: {}, ignore", sessionId);
                return null;
            }
            webSocket.setAttachment(actor);
            actor.attachPlaybackWs(
                    (playbackContext) ->
                            playbackOn2(playbackContext,
                                    actor,
                                    webSocket),
                    (event, payload) -> {
                        try {
                            WSEventVO.sendEvent(webSocket, event, payload);
                        } catch (Exception ex) {
                            log.warn("[{}]: PoActor sendback {}/{} failed, detail: {}", actor.sessionId(), event, payload, ex.toString());
                        }
                    });
            return actor;
        }
    }

    private void handleCommand(final WSCommandVO<Void> cmd, final String message, final WebSocket webSocket, final PoActor actor) throws JsonProcessingException {
        if ("StartTranscription".equals(cmd.getHeader().get("name"))) {
            asrService.startTranscription(VOStartTranscription.of(message), webSocket);
        } else if ("StopTranscription".equals(cmd.getHeader().get("name"))) {
            asrService.stopTranscription(webSocket);
        }else if ("PCMPlaybackStopped".equals(cmd.getHeader().get("name"))) {
            final var vo = VOPCMPlaybackStopped.of(message);
            log.info("[{}]: handlePCMPlaybackStoppedCommand: playbackId: {}", actor.sessionId(), vo.playback_id);
            actor.notifyPlaybackStop(vo.playback_id, vo.content_id, vo.playback_begin_timestamp, vo.playback_end_timestamp, vo.playback_duration);
        } else if ("PCMPlaybackPaused".equals(cmd.getHeader().get("name"))) {
            // eg: {"header": {"name": "PCMPlaybackPaused"},"payload": {"playback_id": "ebfafab3-eb5e-454a-9427-187ceff9ff23", "content_id": "2213745", "playback_duration": "4.410666666666666"}}
            final var vo = VOPCMPlaybackPaused.of(message);
            log.info("[{}]: handlePCMPlaybackPausedCommand: playbackId: {}", actor.sessionId(), vo.playback_id);
            actor.notifyPlaybackPaused(vo.playback_id, vo.content_id, vo.playback_duration);
        } else if ("PCMPlaybackResumed".equals(cmd.getHeader().get("name"))) {
            // eg: {"header": {"name": "PCMPlaybackResumed"},"payload": {"playback_id": "ebfafab3-eb5e-454a-9427-187ceff9ff23", "content_id": "2213745", "playback_duration": "4.410666666666666"}}
            final var vo = VOPCMPlaybackResumed.of(message);
            log.info("[{}]: handlePCMPlaybackResumedCommand: playbackId: {}", actor.sessionId(), vo.playback_id);
            actor.notifyPlaybackResumed(vo.playback_id, vo.content_id, vo.playback_duration);
        } else if ("PCMPlaybackStarted".equals(cmd.getHeader().get("name"))) {
            // eg: {"header": {"name": "PCMPlaybackStarted"},"payload": {"playback_id": "ebfafab3-eb5e-454a-9427-187ceff9ff23", "content_id": "2213745"}}
            final var vo = VOPCMPlaybackStarted.of(message);
            log.info("[{}]: handlePCMPlaybackStartedCommand: playbackId: {}", actor.sessionId(), vo.playback_id);
            actor.notifyPlaybackStarted(vo.playback_id, vo.content_id);
        } else if ("UserAnswer".equals(cmd.getHeader().get("name"))) {
            actor.notifyUserAnswer(VOUserAnswer.of(message));
        } else {
            log.warn("handleCommand: Unknown Command: {}", cmd);
        }
    }

    private Runnable playbackOn2(final PoActor.PlaybackContext playbackContext, final PoActor poActor, final WebSocket webSocket) {
        // interval = 20 ms
        int interval = 20;
        log.info("[{}]: playbackOn2: {} => sample rate: {}/interval: {}/channels: {} as {}",
                poActor.sessionId(), playbackContext.path(), 16000, interval, 1, playbackContext.playbackId());
        final PlayStreamPCMTask2 task = new PlayStreamPCMTask2(
                playbackContext.playbackId(),
                poActor.sessionId(),
                playbackContext.path(),
                schedulerProvider.getObject(),
                new SampleInfo(16000, interval, 16, 1),
                (timestamp) -> {
                    WSEventVO.sendEvent(webSocket, "PCMBegin",
                            new PayloadPCMEvent(playbackContext.playbackId(), playbackContext.contentId()));
                    poActor.notifyPlaybackSendStart(playbackContext.playbackId(), timestamp);
                },
                (timestamp) -> {
                    poActor.notifyPlaybackSendStop(playbackContext.playbackId(), timestamp);
                    WSEventVO.sendEvent(webSocket, "PCMEnd",
                            new PayloadPCMEvent(playbackContext.playbackId(), playbackContext.contentId()));
                },
                (bytes) -> {
                    webSocket.send(bytes);
                    poActor.notifyPlaybackSendData(bytes);
                },
                (_task) -> {
                    log.info("[{}]: PlayStreamPCMTask2 {} send data stopped with completed: {}",
                            poActor.sessionId(), _task, _task.isCompleted());
                }
        );

        final BuildStreamTask bst = bstService.getTaskOf(playbackContext.path(), true, 16000);
        if (bst != null) {
            poActor.notifyPlaybackStart(playbackContext.playbackId());
            bst.buildStream(task::appendData, (ignore)->task.appendCompleted());
        }
        return task::stop;
    }

    private final ObjectProvider<OSS> _ossProvider;

    @Autowired
    private BSTService bstService;

    @Resource
    private CallApi _callApi;

    @Resource
    private ScriptApi _scriptApi;

    @Value("${call.answer_timeout_ms}")
    private long _answer_timeout_ms;

    @Value("${session.check_idle_interval_ms}")
    private long _check_idle_interval_ms;

    @Value("${oss.bucket}")
    private String _oss_bucket;

    @Value("${oss.path}")
    private String _oss_path;

    private final ObjectProvider<ScheduledExecutorService> schedulerProvider;
    private final ObjectProvider<CommandExecutor> cmdExecutorProvider;
    private ExecutorService _ossAccessExecutor;

    @Autowired
    private ASRService asrService;
}
