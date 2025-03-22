package com.yulore.medhub.ws.builder;

import com.aliyun.oss.OSS;
import com.yulore.bst.*;
import com.yulore.medhub.api.CallApi;
import com.yulore.medhub.api.ScriptApi;
import com.yulore.medhub.service.ASRService;
import com.yulore.medhub.service.BSTService;
import com.yulore.util.OrderedTaskExecutor;
import com.yulore.medhub.task.PlayStreamPCMTask2;
import com.yulore.medhub.task.SampleInfo;
import com.yulore.medhub.vo.*;
import com.yulore.medhub.vo.cmd.*;
import com.yulore.medhub.ws.WSCommandRegistry;
import com.yulore.medhub.ws.WsHandler;
import com.yulore.medhub.ws.WsHandlerBuilder;
import com.yulore.medhub.ws.actor.PoActor;
import com.yulore.metric.MetricCustomized;
import com.yulore.util.VarsUtil;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
@Component("po_io")
@ConditionalOnProperty(prefix = "feature", name = "po_io", havingValue = "enabled")
public class PoActorBuilder implements WsHandlerBuilder {
    @PostConstruct
    public void init() {
        cmds.register(VOStartTranscription.TYPE,"StartTranscription",
            ctx-> asrService.startTranscription(ctx.actor(), ctx.payload(), ctx.ws())
                    .handle((timer, ex)->ctx.sample().stop(timer))
            )
            .register(WSCommandVO.WSCMD_VOID,"StopTranscription",
                    ctx-> asrService.stopTranscription(ctx.ws()));

        playback_timer = timerProvider.getObject("mh.playback.delay", MetricCustomized.builder().tags(List.of("actor", "poio")).build());
        transmit_timer = timerProvider.getObject("mh.transmit.delay", MetricCustomized.builder()
                .tags(List.of("actor", "poio"))
                .maximumExpected(Duration.ofMinutes(1))
                .build());
        oss_timer = timerProvider.getObject("oss.upload.duration", MetricCustomized.builder().tags(List.of("actor", "poio")).build());
        gaugeProvider.getObject((Supplier<Number>)_wscount::get, "mh.ws.count", MetricCustomized.builder().tags(List.of("actor", "poio")).build());

        executor = executorProvider.apply("longTimeExecutor");
    }

    // wss://domain/path?uuid=XX&tid=XXX&role=call
    // wss://domain/path?sessionId=xxx&role=playback
    @Override
    public WsHandler build(final String prefix, final WebSocket webSocket, final ClientHandshake handshake) {
        final String path = handshake.getResourceDescriptor();
        final int varsBegin = path.indexOf('?');
        final String role = varsBegin > 0 ? VarsUtil.extractValueWithSplitter(path.substring(varsBegin + 1), "role", '&') : null;

        if ("call".equals(role)) {
            return buildPoActor(prefix, webSocket, handshake, path, varsBegin, role);
        } else {
            return attachPoActor(prefix, webSocket, path, varsBegin, role);
        }
    }

    private PoActor attachPoActor(final String prefix, final WebSocket webSocket, final String path, final int varsBegin, final String role) {
        final String sessionId = varsBegin > 0 ? VarsUtil.extractValueWithSplitter(path.substring(varsBegin + 1), "sessionId", '&') : null;
        // init PlaybackSession attach with webSocket
        log.info("ws path match: {}, role: {}, using ws as PoActor's playback ws: [{}]", prefix, role, sessionId);
        final PoActor actor = PoActor.findBy(sessionId);
        if (actor == null) {
            log.info("can't find callSession by sessionId: {}, ignore", sessionId);
            return null;
        }
        _wscount.incrementAndGet();
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

    private PoActor buildPoActor(final String prefix, final WebSocket webSocket, final ClientHandshake handshake, final String path, final int varsBegin, final String role) {
        _wscount.incrementAndGet();

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
                    final Timer.Sample oss_sample = Timer.start();
                    executor.execute(() -> {
                        ossProvider.getObject().putObject(ctx.bucketName(), ctx.objectName(), ctx.content());
                        oss_sample.stop(oss_timer);
                        log.info("[{}]: upload record to oss => bucket:{}/object:{}, cost {} ms",
                                ctx.sessionId(), ctx.bucketName(), ctx.objectName(), System.currentTimeMillis() - startUploadInMs);
                    });
                },
                _sessionId -> WSEventVO.sendEvent(webSocket, "CallStarted", new PayloadCallStarted(_sessionId))) {

            @Override
            protected WSCommandRegistry<PoActor> commandRegistry() {
                return cmds;
            }

            long totalDelayInMs = 0;

            @Override
            public void onMessage(final WebSocket webSocket, final ByteBuffer bytes, final long timestampInMs) {
                orderedTaskExecutor.submit(actorIdx(), ()-> {
                    if (transmit(bytes)) {
                        totalDelayInMs += System.currentTimeMillis() - timestampInMs;
                        // transmit success
                        if ((transmitCount() % 50) == 0) {
                            transmit_timer.record(totalDelayInMs, TimeUnit.MILLISECONDS);
                            totalDelayInMs = 0;
                            log.info("[{}]: transmit 50 times.", sessionId());
                        }
                    }
                });
            }

            @Override
            public void onClose(final WebSocket webSocket) {
                executor.execute(()-> {
                    _wscount.decrementAndGet();
                    super.onClose(webSocket);
                });
            }
        };
        webSocket.setAttachment(actor);
        actor.onAttached(webSocket);

        actor.scheduleCheckIdle(schedulerProvider.getObject(), _check_idle_interval_ms, actor::checkIdle);
        schedulerProvider.getObject().schedule(actor::notifyMockAnswer, _answer_timeout_ms, TimeUnit.MILLISECONDS);

        log.info("ws path match: {}, using ws as PoActor with role: {}", prefix, role);
        return actor;
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

    private final ObjectProvider<OSS> ossProvider;
    private final BSTService bstService;
    private final ObjectProvider<ScheduledExecutorService> schedulerProvider;
    private final Function<String, Executor> executorProvider;
    private final ASRService asrService;
    private final OrderedTaskExecutor orderedTaskExecutor;

    private final ObjectProvider<Timer> timerProvider;
    private final ObjectProvider<Gauge> gaugeProvider;

    private final AtomicInteger _wscount = new AtomicInteger(0);

    private Executor executor;
    private Timer playback_timer;
    private Timer oss_timer;
    private Timer transmit_timer;

    private final WSCommandRegistry<PoActor> cmds = new WSCommandRegistry<PoActor>()
            .register(VOPCMPlaybackStarted.TYPE,"PCMPlaybackStarted",
                    ctx->ctx.actor().notifyPlaybackStarted(ctx.payload(), playback_timer))
            .register(VOPCMPlaybackStopped.TYPE,"PCMPlaybackStopped",
                      ctx->ctx.actor().notifyPlaybackStop(ctx.payload()))
            .register(VOPCMPlaybackPaused.TYPE,"PCMPlaybackPaused",
                      ctx->ctx.actor().notifyPlaybackPaused(ctx.payload()))
            .register(VOPCMPlaybackResumed.TYPE,"PCMPlaybackResumed",
                      ctx->ctx.actor().notifyPlaybackResumed(ctx.payload()))
            .register(VOUserAnswer.TYPE,"UserAnswer",
                      ctx->ctx.actor().notifyUserAnswer(ctx.payload()))
            ;
}
