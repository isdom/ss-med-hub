package com.yulore.medhub.ws.builder;

import com.alibaba.fastjson.JSON;
import com.aliyun.oss.OSS;
import com.mgnt.utils.StringUnicodeEncoderDecoder;
import com.yulore.bst.BuildStreamTask;
import com.yulore.medhub.api.AIReplyVO;
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
import io.netty.util.concurrent.DefaultThreadFactory;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
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
            ).register(WSCommandVO.WSCMD_VOID,"StopTranscription",
                ctx-> log.info("[{}] ApoActorBuilder: handle StopTranscription cmd",
                        ctx.actor().sessionId())
            );

        playback_timer = timerProvider.getObject("mh.playback.delay", MetricCustomized.builder().tags(List.of("actor", "apo")).build());
        transmit_timer = timerProvider.getObject("mh.transmit.delay", MetricCustomized.builder()
                .tags(List.of("actor", "apo"))
                .maximumExpected(Duration.ofMinutes(1))
                .build());
        oss_timer = timerProvider.getObject("oss.upload.duration", MetricCustomized.builder().tags(List.of("actor", "apo")).build());
        gaugeProvider.getObject((Supplier<Number>)_wscount::get, "mh.ws.count", MetricCustomized.builder().tags(List.of("actor", "apo")).build());
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

    private static final ConcurrentMap<String, ApoActor> _actors = new ConcurrentHashMap<>();

    private WsHandler attachActor(final String prefix,
                                  final WebSocket webSocket,
                                  final String path,
                                  final int varsBegin,
                                  final String role) {
        final String sessionId = varsBegin > 0 ? VarsUtil.extractValueWithSplitter(path.substring(varsBegin + 1), "sessionId", '&') : null;
        // init PlaybackSession attach with webSocket
        log.info("ws path match: {}, role: {}, using ws as ApoActor's playback ws: [{}]", prefix, role, sessionId);
        final var actor = _actors.get(sessionId);
        if (actor == null) {
            log.info("can't find callSession by sessionId: {}, ignore", sessionId);
            return null;
        }

        _wscount.incrementAndGet();
        final var wsh = new WsHandler() {
            @Override
            public void onAttached(WebSocket webSocket) {}

            @Override
            public void onClose(WebSocket webSocket) {
                webSocket.setAttachment(null);
                _wscount.decrementAndGet();
            }

            @Override
            public void onMessage(final WebSocket webSocket, final String message, final Timer.Sample sample) {
                orderedExecutor.submit(actor.actorIdx(), ()-> {
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
                (id, vo) -> reply2playback(id, vo, actor, webSocket),
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

    private WsHandler buildActor(final String prefix,
                                 final WebSocket webSocket,
                                 final ClientHandshake handshake,
                                 final String path,
                                 final int varsBegin,
                                 final String role) {
        _wscount.incrementAndGet();

        // means ws with role: call
        // init ApoActor attach with webSocket
        final String uuid = VarsUtil.extractValueWithSplitter(path.substring(varsBegin + 1), "uuid", '&');
        final String tid = VarsUtil.extractValueWithSplitter(path.substring(varsBegin + 1), "tid", '&');
        final String clientIp = handshake.getFieldValue("X-Forwarded-For");
        final var executor = executorProvider.apply("ltx");

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
            // public long answerInMss() {
            //    return 0;
            //}
            public Consumer<Runnable> runOn(int idx) {
                return runnable -> orderedExecutor.submit(idx, runnable);
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
            public Consumer<ApoActor> callStarted() {
                return actor_ -> {
                    _actors.put(actor_.sessionId(), actor_);
                    WSEventVO.sendEvent(webSocket, "CallStarted", new PayloadCallStarted(actor_.sessionId()));
                };
            }
        });

        final var wsh = new WsHandler() {
            @Override
            public void onAttached(WebSocket webSocket) {}

            @Override
            public void onClose(WebSocket webSocket) {
                webSocket.setAttachment(null);
                orderedExecutor.submit(actor.actorIdx(), ()-> {
                    try {
                        if (actor.sessionId() != null) {
                            _actors.remove(actor.sessionId());
                        }
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
                orderedExecutor.submit(actor.actorIdx(), ()-> {
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
                mediaExecutor.submit(actor.actorIdx(), ()->actor.transmit(buffer));
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

        schedulerProvider.getObject().scheduleWithFixedDelay(actor::checkIdle, _check_idle_interval_ms, _check_idle_interval_ms, TimeUnit.MILLISECONDS);
        schedulerProvider.getObject().schedule(actor::notifyMockAnswer, _answer_timeout_ms, TimeUnit.MILLISECONDS);

        log.info("ws path match: {}, using ws as ApoActor with role: {}", prefix, role);
        return wsh;
    }

    record PlaybackContext(String playbackId, String path, String contentId) {
    }

    private Supplier<Runnable> reply2playback(final String playbackId, final AIReplyVO replyVO, final ApoActor actor, WebSocket webSocket) {
        final String aiContentId = replyVO.getAi_content_id() != null ? Long.toString(replyVO.getAi_content_id()) : null;
        if ("cp".equals(replyVO.getVoiceMode())) {
            return ()->playbackOn(new PlaybackContext(
                    playbackId,
                    String.format("type=cp,%s", JSON.toJSONString(replyVO.getCps())),
                    aiContentId), actor, webSocket);
        } else if ("wav".equals(replyVO.getVoiceMode())) {
            return ()->playbackOn(new PlaybackContext(
                    playbackId,
                    String.format("{cache=true,bucket=%s}%s%s", _oss_bucket, _oss_path, replyVO.getAi_speech_file()),
                    aiContentId), actor, webSocket);
        } else if ("tts".equals(replyVO.getVoiceMode())) {
            return ()->playbackOn(new PlaybackContext(
                    playbackId,
                    String.format("{type=tts,text=%s}tts.wav",
                            StringUnicodeEncoderDecoder.encodeStringToUnicodeSequence(replyVO.getReply_content())),
                    aiContentId), actor, webSocket);
        } else {
            return null;
        }
    }

    private Runnable playbackOn(final PlaybackContext playbackContext, final ApoActor actor, final WebSocket webSocket) {
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
                timestamp -> {
                    WSEventVO.sendEvent(webSocket, "PCMBegin",
                            new PayloadPCMEvent(playbackContext.playbackId(), playbackContext.contentId()));
                    actor.notifyPlaybackSendStart(playbackContext.playbackId(), timestamp);
                },
                timestamp -> {
                    actor.notifyPlaybackSendStop(playbackContext.playbackId(), timestamp);
                    WSEventVO.sendEvent(webSocket, "PCMEnd",
                            new PayloadPCMEvent(playbackContext.playbackId(), playbackContext.contentId()));
                },
                bytes -> {
                    webSocket.send(bytes);
                    actor.notifyPlaybackSendData(bytes);
                },
                _task -> {
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

    final static class MediaExecutor implements OrderedExecutor {
        // 使用连接ID的哈希绑定固定线程
        private static final int POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;

        private final ExecutorService[] workers = new ExecutorService[POOL_SIZE];

        MediaExecutor() {
            for (int i = 0; i < POOL_SIZE; i++) {
                final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1,
                        0L, TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        new DefaultThreadFactory("Handle-Media-" + i));
                workers[i] = executor;
                log.info("create Handle-Media-{}", i);
            }
        }

        public void release() {
            for (int i = 0; i < POOL_SIZE; i++) {
                workers[i].shutdownNow();
            }
        }

        @Override
        public void submit(final int idx, final Runnable task) {
            final int order = idx % POOL_SIZE;
            workers[order].submit(task);
        }

        @Override
        public int idx2order(final int idx) {
            return idx % POOL_SIZE;
        }
    }

    private final MediaExecutor mediaExecutor = new MediaExecutor();
}