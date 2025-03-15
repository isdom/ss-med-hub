package com.yulore.medhub.ws.builder;

import com.aliyun.oss.OSS;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import com.yulore.medhub.ws.WSCommandRegistry;
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

            final WSCommandRegistry<PoActor> cmds = new WSCommandRegistry<>();

            cmds.register(VOStartTranscription.TYPE,"StartTranscription",
                            ctx-> asrService.startTranscription(ctx.payload(), ctx.ws()))
                    .register(WSCommandVO.WSCMD_VOID,"StopTranscription",
                            ctx-> asrService.stopTranscription(ctx.ws()))
                    .register(VOPCMPlaybackStopped.TYPE,"PCMPlaybackStopped",
                            ctx->ctx.actor().notifyPlaybackStop(
                                    ctx.payload().playback_id,
                                    ctx.payload().content_id,
                                    ctx.payload().playback_begin_timestamp,
                                    ctx.payload().playback_end_timestamp,
                                    ctx.payload().playback_duration))
                    .register(VOPCMPlaybackPaused.TYPE,"PCMPlaybackPaused",
                            ctx->ctx.actor().notifyPlaybackPaused(
                                    ctx.payload().playback_id,
                                    ctx.payload().content_id,
                                    ctx.payload().playback_duration))
                    .register(VOPCMPlaybackResumed.TYPE,"PCMPlaybackResumed",
                            ctx->ctx.actor().notifyPlaybackResumed(
                                    ctx.payload().playback_id,
                                    ctx.payload().content_id,
                                    ctx.payload().playback_duration))
                    .register(VOPCMPlaybackStarted.TYPE,"PCMPlaybackStarted",
                            ctx->ctx.actor().notifyPlaybackStarted(
                                    ctx.payload().playback_id,
                                    ctx.payload().content_id))
                    .register(VOUserAnswer.TYPE,"UserAnswer",
                            ctx->ctx.actor().notifyUserAnswer(ctx.payload()))
                    ;

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
                            cmds.handleCommand(WSCommandVO.parse(message, WSCommandVO.WSCMD_VOID), message, this, webSocket);
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
                            playbackOn(playbackContext,
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

    private Runnable playbackOn(final PoActor.PlaybackContext playbackContext, final PoActor poActor, final WebSocket webSocket) {
        // interval = 20 ms
        int interval = 20;
        log.info("[{}]: playbackOn: {} => sample rate: {}/interval: {}/channels: {} as {}",
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
