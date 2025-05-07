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
import com.yulore.metric.MetricCustomized;
import com.yulore.util.ExceptionUtil;
import com.yulore.util.OrderedExecutor;
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
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
            idx2actor.put(vo.localIdx, actor);
            actor.startTranscription();
        }

        void removeLocal(final AFSRemoveLocalCommand vo, final Timer timer) {
            timer.record(System.currentTimeMillis() - vo.hangupInMss / 1000L, TimeUnit.MILLISECONDS);

            final var actor = idx2actor.remove(vo.localIdx);
            if (actor != null) {
                actor.close();
            }
            log.info("AfsIO: removeLocal {}", vo.localIdx);
        }

        void playbackStarted(final AFSPlaybackStarted vo, final Timer reaction_timer, final Timer delay_timer) {
            final long now = System.currentTimeMillis();
            reaction_timer.record(vo.eventInMss - vo.startInMss, TimeUnit.MICROSECONDS);
            delay_timer.record(now - vo.startInMss / 1000L, TimeUnit.MILLISECONDS);
            final var actor = idx2actor.get(vo.localIdx);
            if (actor != null) {
                actor.playbackStarted(vo);
            }
            log.info("AfsIO: playbackStarted {}", vo);
        }

        void playbackStopped(final AFSPlaybackStopped vo) {
            final var actor = idx2actor.get(vo.localIdx);
            if (actor != null) {
                actor.playbackStopped(vo);
            }
            log.info("AfsIO: playbackStopped {}", vo);
        }

        AfsActor actorOf(final int localIdx) {
            return idx2actor.get(localIdx);
        }

        final ConcurrentMap<Integer, AfsActor> idx2actor = new ConcurrentHashMap<>();
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

            @Override
            public void onMessage(final WebSocket webSocket, final ByteBuffer buffer, final long recvdInMs) {
                final byte[] byte4 = new byte[4];
                buffer.get(byte4, 0, 4);
                // 将小端字节序转换为 int
                final int localIdx =
                        ((byte4[3] & 0xFF) << 24) |
                        ((byte4[2] & 0xFF) << 16) |
                        ((byte4[1] & 0xFF) << 8)  |
                        (byte4[0] & 0xFF);          // 最低有效字节（小端的第一个字节）
                orderedExecutor.submit(localIdx, ()->actorOf(localIdx).transmit(buffer, recvdInMs, td_timer, hc_timer));
            }

            @Override
            public void onAttached(WebSocket webSocket) {
            }

            @Override
            public void onClose(final WebSocket webSocket) {
                // TODO: close all session create by this actor
                _wscount.decrementAndGet();
                log.info("afs_io onClose {}: ", webSocket);
            }
        };

        session_gauges.computeIfAbsent(ipv4,
        ip -> gaugeProvider.getObject((Supplier<Number>)afs.idx2actor::size, "mh.afs.session",
                MetricCustomized.builder()
                        .tags(List.of("afs", ip))
                        .build()));

        webSocket.setAttachment(afs);
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

    private final ObjectProvider<AfsActor> afsProvider;
    private final ObjectProvider<HandlerUrlBuilder> urlProvider;
    private final OrderedExecutor orderedExecutor;
    private final ObjectProvider<Timer> timerProvider;
    private final ObjectProvider<Gauge> gaugeProvider;

    private final AtomicInteger _wscount = new AtomicInteger(0);

    private final ConcurrentMap<String, Timer> transmit_delay_timers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> handle_cost_timers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> answer_delay_timers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> hangup_delay_timers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> playback_reaction_timers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> playback_delay_timers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Gauge> session_gauges = new ConcurrentHashMap<>();
}
