package com.yulore.medhub.ws.builder;

import com.aliyun.oss.OSS;
import com.yulore.bst.BuildStreamTask;
import com.yulore.medhub.service.BSTService;
import com.yulore.medhub.task.PlayStreamPCMTask2;
import com.yulore.medhub.task.SampleInfo;
import com.yulore.medhub.vo.*;
import com.yulore.medhub.vo.cmd.*;
import com.yulore.medhub.ws.WSCommandRegistry;
import com.yulore.medhub.ws.WsHandler;
import com.yulore.medhub.ws.WsHandlerBuilder;
import com.yulore.medhub.ws.actor.ApoActor;
import com.yulore.metric.DisposableGauge;
import com.yulore.metric.MetricCustomized;
import com.yulore.util.ExceptionUtil;
import com.yulore.util.OrderedExecutor;
import com.yulore.util.VarsUtil;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;
import org.java_websocket.exceptions.WebsocketNotConnectedException;
import org.java_websocket.handshake.ClientHandshake;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
@Component("apo_io")
@ConditionalOnProperty(prefix = "feature", name = "apo_io", havingValue = "enabled")
public class ApoActorBuilder implements WsHandlerBuilder {
    @PostConstruct
    public void init() {
        cmds.register(VOStartTranscription.TYPE,"StartTranscription",
            ctx->
                    //asrService.startTranscription(ctx.actor(), ctx.payload(), ctx.ws())
                    //.handle((timer, ex)->ctx.sample().stop(timer))
                ctx.actor().startTranscription()
            );
            /*.register(WSCommandVO.WSCMD_VOID,"StopTranscription",
                    ctx-> asrService.stopTranscription(ctx.ws()))
                    ;
             */

        playback_timer = timerProvider.getObject("mh.playback.delay", MetricCustomized.builder().tags(List.of("actor", "apo")).build());
        transmit_timer = timerProvider.getObject("mh.transmit.delay", MetricCustomized.builder()
                .tags(List.of("actor", "apo"))
                .maximumExpected(Duration.ofMinutes(1))
                .build());
        oss_timer = timerProvider.getObject("oss.upload.duration", MetricCustomized.builder().tags(List.of("actor", "apo")).build());
        gaugeProvider.getObject((Supplier<Number>)_wscount::get, "mh.ws.count", MetricCustomized.builder().tags(List.of("actor", "apo")).build());

        executor = executorProvider.apply("ltx");
    }

    // wss://domain/path?uuid=XX&tid=XXX&role=call
    // wss://domain/path?sessionId=xxx&role=playback
    @Override
    public WsHandler build(final String prefix, final WebSocket webSocket, final ClientHandshake handshake) {
        final String path = handshake.getResourceDescriptor();
        final int varsBegin = path.indexOf('?');
        final String role = varsBegin > 0 ? VarsUtil.extractValueWithSplitter(path.substring(varsBegin + 1), "role", '&') : null;

        if ("call".equals(role)) {
            return buildActor(prefix, webSocket, handshake, path, varsBegin, role);
        } else {
            return attachActor(prefix, webSocket, path, varsBegin, role);
        }
    }

    private WsHandler attachActor(final String prefix, final WebSocket webSocket, final String path, final int varsBegin, final String role) {
        final String sessionId = varsBegin > 0 ? VarsUtil.extractValueWithSplitter(path.substring(varsBegin + 1), "sessionId", '&') : null;
        // init PlaybackSession attach with webSocket
        log.info("ws path match: {}, role: {}, using ws as ApoActor's playback ws: [{}]", prefix, role, sessionId);
        final var actor = ApoActor.findBy(sessionId);
        if (actor == null) {
            log.info("can't find callSession by sessionId: {}, ignore", sessionId);
            return null;
        }

        _wscount.incrementAndGet();
        final Executor wsmsg = executorProvider.apply("wsmsg");
        final var wsh = new WsHandler() {
            @Override
            public void onAttached(WebSocket webSocket) {}

            @Override
            public void onClose(WebSocket webSocket) {
                webSocket.setAttachment(null);
                _wscount.decrementAndGet();
            }

            @Override
            public void onMessage(WebSocket webSocket, String message, Timer.Sample sample) {
                wsmsg.execute(()-> {
                    try {
                        cmds.handleCommand(WSCommandVO.parse(message, WSCommandVO.WSCMD_VOID), message, actor, webSocket, sample);
                    } catch (Exception ex) {
                        log.error("handleCommand {}: {}, an error occurred: {}",
                                webSocket.getRemoteSocketAddress(), message, ExceptionUtil.exception2detail(ex));
                    }
                });
            }

            @Override
            public void onWebsocketError(final Exception ex) {
                log.warn("[{}]: [{}]-[{}]: exception_detail: (callStack)\n{}\n{}", actor.clientIp(), actor.sessionId(), actor.uuid(),
                        ExceptionUtil.dumpCallStack(ex, null, 0),
                        ExceptionUtil.exception2detail(ex));
            }
        };
        webSocket.setAttachment(wsh);

        actor.attachPlaybackWs(
                playbackContext ->
                        playbackOn(playbackContext,
                                actor,
                                webSocket),
                (event, payload) -> {
                    try {
                        WSEventVO.sendEvent(webSocket, event, payload);
                    } catch (WebsocketNotConnectedException ex) {
                        log.info("[{}]: ApoActor sendback {}/{} with WebsocketNotConnectedException", actor.sessionId(), event, payload);
                    } catch (Exception ex) {
                        log.warn("[{}]: ApoActor sendback {}/{} failed, detail: {}", actor.sessionId(), event, payload,
                                ExceptionUtil.exception2detail(ex));
                    }
                });
        return wsh;
    }

    private WsHandler buildActor(final String prefix, final WebSocket webSocket, final ClientHandshake handshake, final String path, final int varsBegin, final String role) {
        _wscount.incrementAndGet();

        // means ws with role: call
        // init ApoActor attach with webSocket
        final String uuid = VarsUtil.extractValueWithSplitter(path.substring(varsBegin + 1), "uuid", '&');
        final String tid = VarsUtil.extractValueWithSplitter(path.substring(varsBegin + 1), "tid", '&');
        final String clientIp = handshake.getFieldValue("X-Forwarded-For");
        final Executor wsmsg = executorProvider.apply("wsmsg");
        final var actor = apoProvider.getObject(new ApoActor.Context() {
            public String clientIp() {
                return clientIp;
            }
            public String uuid() {
                return uuid;
            }
            public String tid() {
                return tid;
            }
            public long answerInMss() {
                return 0;
            }
            public int idleTimeout() {
                return 0;
            }
            public String bucket() {
                return _oss_bucket;
            }
            public String wavPath() {
                return _oss_path;
            }
            public Consumer<Runnable> runOn() {
                return null;
            }
            public Consumer<ApoActor> doHangup() {
                return actor_ -> {
                    try {
                        WSEventVO.sendEvent(webSocket, "CallEnded", new PayloadCallEnded(actor_.sessionId()));
                        log.info("[{}]: sendback CallEnded event", actor_.sessionId());
                    } catch (Exception ex) {
                        log.warn("[{}]: sendback CallEnded event failed, detail: {}", actor_.sessionId(), ex.toString());
                    }
                };
            }
            public BiConsumer<String, Object> sendEvent() {
                return null;
            }
            public Consumer<ApoActor.RecordContext> saveRecord() {
                return ctx -> {
                    final long startUploadInMs = System.currentTimeMillis();
                    final Timer.Sample oss_sample = Timer.start();
                    executor.execute(() -> {
                        ossProvider.getObject().putObject(ctx.bucketName(), ctx.objectName(), ctx.content());
                        oss_sample.stop(oss_timer);
                        log.info("[{}]: upload record to oss => bucket:{}/object:{}, cost {} ms",
                                ctx.sessionId(), ctx.bucketName(), ctx.objectName(), System.currentTimeMillis() - startUploadInMs);
                    });
                };
            }
            public Consumer<String> callStarted() {
                return sessionId_ -> WSEventVO.sendEvent(webSocket, "CallStarted", new PayloadCallStarted(sessionId_));
            }
        });

        final var wsh = new WsHandler() {
            @Override
            public void onAttached(WebSocket webSocket) {}

            @Override
            public void onClose(WebSocket webSocket) {
                webSocket.setAttachment(null);
                executor.execute(()-> {
                    try {
                        actor.close();
                    } catch (Exception ex) {
                        log.warn("[{}] ApoActor.close() with exception: {}", actor.sessionId(), ExceptionUtil.exception2detail(ex));
                    } finally {
                        _wscount.decrementAndGet();
                    }
                });
            }

            @Override
            public void onMessage(final WebSocket webSocket, final String message, final Timer.Sample sample) {
                wsmsg.execute(()-> {
                    try {
                        cmds.handleCommand(WSCommandVO.parse(message, WSCommandVO.WSCMD_VOID), message, actor, webSocket, sample);
                    } catch (Exception ex) {
                        log.error("handleCommand {}: {}, an error occurred: {}",
                                webSocket.getRemoteSocketAddress(), message, ExceptionUtil.exception2detail(ex));
                    }
                });
            }

            @Override
            public void onMessage(final WebSocket webSocket, final ByteBuffer buffer, final long timestampInMs) {
                orderedExecutor.submit(actor.actorIdx(), ()->actor.transmit(buffer));
            }

            @Override
            public void onWebsocketError(final Exception ex) {
                log.warn("[{}]: [{}]-[{}]: exception_detail: (callStack)\n{}\n{}", actor.clientIp(), actor.sessionId(), actor.uuid(),
                        ExceptionUtil.dumpCallStack(ex, null, 0),
                        ExceptionUtil.exception2detail(ex));
            }
        };

        webSocket.setAttachment(wsh);
        // wsh.onAttached(webSocket);

        // actor.scheduleCheckIdle(schedulerProvider.getObject(), _check_idle_interval_ms, actor::checkIdle);
        schedulerProvider.getObject().scheduleWithFixedDelay(actor::checkIdle, _check_idle_interval_ms, _check_idle_interval_ms, TimeUnit.MILLISECONDS);
        schedulerProvider.getObject().schedule(actor::notifyMockAnswer, _answer_timeout_ms, TimeUnit.MILLISECONDS);

        log.info("ws path match: {}, using ws as ApoActor with role: {}", prefix, role);
        return wsh;
    }

    private Runnable playbackOn(final ApoActor.PlaybackContext playbackContext, final ApoActor actor, final WebSocket webSocket) {
        // interval = 20 ms
        int interval = 20;
        log.info("[{}]: playbackOn: {} => sample rate: {}/interval: {}/channels: {} as {}",
                actor.sessionId(), playbackContext.path(), 16000, interval, 1, playbackContext.playbackId());
        final PlayStreamPCMTask2 task = new PlayStreamPCMTask2(
                playbackContext.playbackId(),
                actor.sessionId(),
                playbackContext.path(),
                schedulerProvider.getObject(),
                new SampleInfo(16000, interval, 16, 1),
                (timestamp) -> {
                    WSEventVO.sendEvent(webSocket, "PCMBegin",
                            new PayloadPCMEvent(playbackContext.playbackId(), playbackContext.contentId()));
                    actor.notifyPlaybackSendStart(playbackContext.playbackId(), timestamp);
                },
                (timestamp) -> {
                    actor.notifyPlaybackSendStop(playbackContext.playbackId(), timestamp);
                    WSEventVO.sendEvent(webSocket, "PCMEnd",
                            new PayloadPCMEvent(playbackContext.playbackId(), playbackContext.contentId()));
                },
                (bytes) -> {
                    webSocket.send(bytes);
                    actor.notifyPlaybackSendData(bytes);
                },
                (_task) -> {
                    log.info("[{}]: PlayStreamPCMTask2 {} send data stopped with completed: {}",
                            actor.sessionId(), _task, _task.isCompleted());
                }
        );

        final BuildStreamTask bst = bstService.getTaskOf(playbackContext.path(), true, 16000);
        if (bst != null) {
            actor.notifyPlaybackStart(playbackContext.playbackId());
            bst.buildStream(task::appendData, (ignore)->task.appendCompleted());
        }
        return task::stop;
    }

    @Value("${call.answer_timeout_ms}")
    private long _answer_timeout_ms;

    @Value("${session.check_idle_interval_ms}")
    private long _check_idle_interval_ms;

    @Value("${oss.bucket}")
    private String _oss_bucket;

    @Value("${oss.path}")
    private String _oss_path;

    private final ObjectProvider<ApoActor> apoProvider;

    private final ObjectProvider<OSS> ossProvider;
    private final BSTService bstService;
    private final ObjectProvider<ScheduledExecutorService> schedulerProvider;
    private final Function<String, Executor> executorProvider;
    private final OrderedExecutor orderedExecutor;

    private final ObjectProvider<Timer> timerProvider;
    private final ObjectProvider<DisposableGauge> gaugeProvider;

    private final AtomicInteger _wscount = new AtomicInteger(0);

    private Executor executor;
    private Timer playback_timer;
    private Timer oss_timer;
    private Timer transmit_timer;

    private final WSCommandRegistry<ApoActor> cmds = new WSCommandRegistry<ApoActor>()
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