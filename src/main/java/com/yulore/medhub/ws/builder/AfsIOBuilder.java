package com.yulore.medhub.ws.builder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mgnt.utils.StringUnicodeEncoderDecoder;
import com.yulore.medhub.api.AIReplyVO;
import com.yulore.medhub.vo.WSCommandVO;
import com.yulore.medhub.vo.WSEventVO;
import com.yulore.medhub.vo.cmd.*;
import com.yulore.medhub.ws.HandlerUrlBuilder;
import com.yulore.medhub.ws.WSCommandRegistry;
import com.yulore.medhub.ws.WsHandler;
import com.yulore.medhub.ws.WsHandlerBuilder;
import com.yulore.medhub.ws.actor.AfsActor;
import com.yulore.metric.DisposableGauge;
import com.yulore.metric.MetricCustomized;
import com.yulore.util.ByteUtil;
import com.yulore.util.ExceptionUtil;
import com.yulore.util.OrderedExecutor;
import io.micrometer.core.instrument.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;

@Slf4j
@RequiredArgsConstructor
@Component("afs_io")
@ConditionalOnProperty(prefix = "feature", name = "afs_io", havingValue = "enabled")
public class AfsIOBuilder implements WsHandlerBuilder {

    @PostConstruct
    private void init() {
        gaugeProvider.getObject((Supplier<Number>)_wscount::get, "mh.ws.count", MetricCustomized.builder().tags(List.of("actor", "afs_io")).build());
    }

    abstract class AfsIO implements WsHandler {
        AfsIO() {
            for (int idx = 0; idx < MAX_IDX; idx++) {
                idx2agent[idx] = new AtomicReference<>(null);
            }
        }

        AfsActor idx2actor(final int localIdx) {
            if (localIdx > 0 && localIdx<= MAX_IDX) {
                return idx2agent[localIdx - 1].get();
            } else {
                throw new ArrayIndexOutOfBoundsException("idx2actor:"+localIdx);
            }
        }

        private void saveActor(final int localIdx, final AfsActor actor) {
                idx2agent[localIdx - 1].set(actor);
        }

        private AfsActor removeActor(final int localIdx) {
            return idx2agent[localIdx - 1].getAndSet(null);
        }

        void addLocal(final AFSAddLocalCommand vo, final WebSocket ws, final Timer timer) {
            timer.record(System.currentTimeMillis() - vo.answerInMss / 1000L, TimeUnit.MILLISECONDS);

            log.info("AfsIO => addLocal: {}", vo);
            final var actor = afsProvider.getObject(new AfsActor.Context() {
                public int localIdx() {
                    return vo.localIdx;
                }
                public String uuid() {
                    return vo.uuid;
                }
                public String sessionId() {
                    return vo.sessionId;
                }
                public String welcome() {
                    return vo.welcome;
                }
                public long answerInMss() {
                    return vo.answerInMss;
                }
                public int idleTimeout() {
                    return vo.idleTimeout;
                }
                public Consumer<Runnable> runOn() {
                    return runnable -> orderedExecutor.submit(vo.localIdx, runnable);
                }
                public BiFunction<AIReplyVO, Supplier<String>, String> reply2Rms() {
                    return (reply, vars) -> reply2rms(vo.uuid, reply, vars);
                }
                public BiConsumer<String, Object> sendEvent() {
                    return (name,obj)-> WSEventVO.sendEvent(ws, name, obj);
                }
            });
            actorCount.incrementAndGet();
            saveActor(vo.localIdx, actor);
            actor.startTranscription();
        }

        void removeLocal(final AFSRemoveLocalCommand vo, final Timer timer) {
            timer.record(System.currentTimeMillis() - vo.hangupInMss / 1000L, TimeUnit.MILLISECONDS);

            actorCount.decrementAndGet();
            final var actor = removeActor(vo.localIdx);
            if (actor != null) {
                actor.close();
            }
            log.info("AfsIO: removeLocal {}", vo.localIdx);
        }

        void playbackStarted(final AFSPlaybackStarted vo, final Timer reaction_timer, final Timer delay_timer) {
            final long now = System.currentTimeMillis();
            reaction_timer.record(vo.eventInMss - vo.startInMss, TimeUnit.MICROSECONDS);
            delay_timer.record(now - vo.startInMss / 1000L, TimeUnit.MILLISECONDS);
            final var actor = idx2actor(vo.localIdx);
            if (actor != null) {
                actor.playbackStarted(vo);
            }
            log.info("AfsIO: playbackStarted {}", vo);
        }

        void playbackStopped(final AFSPlaybackStopped vo) {
            final var actor = idx2actor(vo.localIdx);
            if (actor != null) {
                actor.playbackStopped(vo);
            }
            log.info("AfsIO: playbackStopped {}", vo);
        }

        private final int MAX_IDX = 4096;

        @SuppressWarnings("unchecked")
        final AtomicReference<AfsActor>[] idx2agent = new AtomicReference[MAX_IDX];
        final AtomicInteger actorCount = new AtomicInteger(0);
    }

    private final Collection<AfsIO> _allAfs = new ConcurrentLinkedQueue<>();

    @Scheduled(fixedDelay = 1_000)  // 每1秒推送一次
    private void checkIdleForAll() {
        for (var afs : _allAfs) {
            for (var ref : afs.idx2agent) {
                final var actor = ref.get();
                if (actor != null) {
                    orderedExecutor.submit(actor.localIdx(), actor::checkIdle);
                }
            }
        }
    }

    @Override
    public WsHandler build(final String prefix, final WebSocket webSocket, final ClientHandshake handshake) {
        final String ipv4 = webSocket.getRemoteSocketAddress().getAddress().getHostAddress();
        final Timer td_timer = transmit_delay_timers.computeIfAbsent(ipv4,
                ip -> timerProvider.getObject("mh.afs.asr.transmit.delay",
                        MetricCustomized.builder()
                                .maximumExpected(Duration.ofMinutes(1))
                                .tags(List.of("afs", ip))
                                .build()));
        final Timer hc_timer = handle_cost_timers.computeIfAbsent(ipv4,
                ip -> timerProvider.getObject("mh.afs.asr.handle.cost",
                        MetricCustomized.builder()
                                .maximumExpected(Duration.ofMinutes(1))
                                .tags(List.of("afs", ip)).build()));
        final Timer answer_delay_timer = answer_delay_timers.computeIfAbsent(ipv4,
                ip -> timerProvider.getObject("mh.afs.cmd.answer.delay",
                        MetricCustomized.builder()
                                .maximumExpected(Duration.ofMinutes(1))
                                .tags(List.of("afs", ip))
                                .build()));
        final Timer hangup_delay_timer = hangup_delay_timers.computeIfAbsent(ipv4,
                ip -> timerProvider.getObject("mh.afs.cmd.hangup.delay",
                        MetricCustomized.builder()
                                .maximumExpected(Duration.ofMinutes(1))
                                .tags(List.of("afs", ip))
                                .build()));
        final Timer playback_reaction_timer = playback_reaction_timers.computeIfAbsent(ipv4,
                ip -> timerProvider.getObject("mh.afs.cmd.playback.reaction",
                        MetricCustomized.builder()
                                .maximumExpected(Duration.ofMinutes(1))
                                .tags(List.of("afs", ip))
                                .build()));
        final Timer playback_delay_timer = playback_delay_timers.computeIfAbsent(ipv4,
                ip -> timerProvider.getObject("mh.afs.cmd.playback.delay",
                        MetricCustomized.builder()
                                .maximumExpected(Duration.ofMinutes(1))
                                .tags(List.of("afs", ip))
                                .build()));

        final Timer frame_cost_timer = frame_cost_timers.computeIfAbsent(ipv4,
                ip -> timerProvider.getObject("mh.afs.asr.frame.cost",
                        MetricCustomized.builder()
                                .tags(List.of("afs", ip))
                                .build()));

        final WSCommandRegistry<AfsIO> cmds = new WSCommandRegistry<AfsIO>()
                .register(AFSAddLocalCommand.TYPE,"AddLocal",
                        ctx->ctx.actor().addLocal(ctx.payload(), ctx.ws(), answer_delay_timer))
                .register(AFSRemoveLocalCommand.TYPE,"RemoveLocal",
                        ctx->ctx.actor().removeLocal(ctx.payload(), hangup_delay_timer))
                .register(AFSPlaybackStarted.TYPE,"PlaybackStarted",
                        ctx->ctx.actor().playbackStarted(ctx.payload(), playback_reaction_timer, playback_delay_timer))
                .register(AFSPlaybackStopped.TYPE,"PlaybackStopped",
                        ctx->ctx.actor().playbackStopped(ctx.payload()))
                ;

        _wscount.incrementAndGet();
        final var afs = new AfsIO() {

            @Override
            public void onMessage(final WebSocket webSocket, final String message, final Timer.Sample sample) {
                try {
                    final var cmd = WSCommandVO.parse(message, AFSCommand.TYPE);
                    orderedExecutor.submit(cmd.payload.localIdx, ()->{
                        try {
                            cmds.handleCommand(cmd, message, this, webSocket, sample);
                        } catch (Exception ex) {
                            log.warn("handleCommand {}: {}, an error occurred: {}",
                                    webSocket.getRemoteSocketAddress(), message, ExceptionUtil.exception2detail(ex));
                        }
                    });
                } catch (Exception ex) {
                    log.error("handleCommand {}: {}, an error occurred: {}",
                            webSocket.getRemoteSocketAddress(), message, ExceptionUtil.exception2detail(ex));
                }
            }

            final byte[] bytes14 = new byte[14];

            final AtomicInteger blk_cnt = new AtomicInteger(0);

            final DisposableGauge session_gauge = gaugeProvider.getObject((Supplier<Number>)actorCount::get, "mh.afs.session",
                    MetricCustomized.builder().tags(List.of("afs", ipv4)).build());

            final DisposableGauge blkcnt_gauge = gaugeProvider.getObject((Supplier<Number>)blk_cnt::get, "mh.afs.asr.frame.blk",
                    MetricCustomized.builder().tags(List.of("afs", ipv4)).build());

            @Override
            public void onMessage(final WebSocket webSocket, final ByteBuffer buffer, final long recvdInMs) {
                int cnt = 0;
                while (buffer.remaining() > 0) {
                    buffer.get(bytes14);
                    final int len = ByteUtil._2BLE_to_int16(bytes14, 0);
                    // 将小端字节序转换为 int
                    final int localIdx = ByteUtil._4BLE_to_int32(bytes14, 2);
                    // 将小端字节序转换为 long
                    final long fsReadFrameInMss = ByteUtil._8BLE_to_long(bytes14, 6);

                    final byte[] data = new byte[len - 4 - 8];
                    buffer.get(data);
                    mediaExecutor.submit(
                            localIdx, ()->{
                                final var actor = idx2actor(localIdx);
                                if (null != actor) {
                                    actor.transmit(data, fsReadFrameInMss, recvdInMs, td_timer, hc_timer);
                                }
                            });
                    cnt++;
                }
                blk_cnt.set(cnt);
                frame_cost_timer.record(System.currentTimeMillis() - recvdInMs, TimeUnit.MILLISECONDS);
            }

            @Override
            public void onAttached(WebSocket webSocket) {
            }

            @Override
            public void onClose(final WebSocket webSocket) {
                _allAfs.remove(this);

                // TODO: close all session create by this actor, 2025-05-16, Very important!
                for (var ref : idx2agent) {
                    final var actor = ref.get();
                    if (actor != null) {
                        orderedExecutor.submit(actor.localIdx(), actor::close);
                    }
                }
                actorCount.set(0);

                _wscount.decrementAndGet();
                log.info("afs_io onClose {}: ", webSocket);
                // remove gauges binding to this AfsIO instance
                session_gauge.dispose();
                blkcnt_gauge.dispose();
            }
        };

        webSocket.setAttachment(afs);
        _allAfs.add(afs);

        log.info("afs_io connected {}", handshake);
        return afs;
    }

    @Value("${rms.cp_prefix}")
    private String _rms_cp_prefix;

    @Value("${rms.tts_prefix}")
    private String _rms_tts_prefix;

    @Value("${rms.wav_prefix}")
    private String _rms_wav_prefix;

    private String reply2rms(final String uuid, final AIReplyVO vo, final Supplier<String> vars) {
        if ("cp".equals(vo.getVoiceMode())) {
            return _rms_cp_prefix.replace("{cpvars}", tryExtractCVOS(vo))
                    .replace("{uuid}", uuid)
                    .replace("{rrms}", urlProvider.getObject().get("read_rms"))
                    .replace("{vars}", vars.get())
                    + "cps.wav"
                    ;
        }

        if ("tts".equals(vo.getVoiceMode())) {
            return _rms_tts_prefix
                    .replace("{uuid}", uuid)
                    .replace("{rrms}", urlProvider.getObject().get("read_rms"))
                    .replace("{vars}", String.format("text=%s,%s",
                            StringUnicodeEncoderDecoder.encodeStringToUnicodeSequence(vo.getReply_content()),
                            vars.get()))
                    + "tts.wav"
                    ;
        }

        if ("wav".equals(vo.getVoiceMode())) {
            return _rms_wav_prefix
                    .replace("{uuid}", uuid)
                    .replace("{rrms}", urlProvider.getObject().get("read_rms"))
                    .replace("{vars}", vars.get())
                    + vo.getAi_speech_file();
        }

        return null;
    }

    private String tryExtractCVOS(final AIReplyVO vo) {
        try {
            return new ObjectMapper().writeValueAsString(vo.getCps());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

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

    private final ObjectProvider<AfsActor> afsProvider;
    private final ObjectProvider<HandlerUrlBuilder> urlProvider;
    private final OrderedExecutor orderedExecutor;
    private final ObjectProvider<Timer> timerProvider;
    private final ObjectProvider<DisposableGauge> gaugeProvider;

    private final AtomicInteger _wscount = new AtomicInteger(0);

    private final ConcurrentMap<String, Timer> transmit_delay_timers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> handle_cost_timers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> answer_delay_timers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> hangup_delay_timers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> playback_reaction_timers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> playback_delay_timers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> frame_cost_timers = new ConcurrentHashMap<>();
}
