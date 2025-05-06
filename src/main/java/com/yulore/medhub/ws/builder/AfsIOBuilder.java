package com.yulore.medhub.ws.builder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mgnt.utils.StringUnicodeEncoderDecoder;
import com.yulore.medhub.api.AIReplyVO;
import com.yulore.medhub.vo.WSEventVO;
import com.yulore.medhub.vo.cmd.AFSAddLocalCommand;
import com.yulore.medhub.vo.cmd.AFSPlaybackStarted;
import com.yulore.medhub.vo.cmd.AFSRemoveLocalCommand;
import com.yulore.medhub.ws.HandlerUrlBuilder;
import com.yulore.medhub.ws.WSCommandRegistry;
import com.yulore.medhub.ws.WsHandler;
import com.yulore.medhub.ws.WsHandlerBuilder;
import com.yulore.medhub.ws.actor.AfsActor;
import com.yulore.medhub.ws.actor.CommandHandler;
import com.yulore.metric.MetricCustomized;
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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.*;

@Slf4j
@RequiredArgsConstructor
@Component("afs_io")
@ConditionalOnProperty(prefix = "feature", name = "afs_io", havingValue = "enabled")
public class AfsIOBuilder implements WsHandlerBuilder {

    private final WSCommandRegistry<AfsIO> cmds = new WSCommandRegistry<AfsIO>()
            .register(AFSAddLocalCommand.TYPE,"AddLocal", ctx->ctx.actor().addLocal(ctx.payload(), ctx.ws()))
            .register(AFSRemoveLocalCommand.TYPE,"RemoveLocal", ctx->ctx.actor().removeLocal(ctx.payload()))
            .register(AFSPlaybackStarted.TYPE,"PlaybackStarted", ctx->ctx.actor().playbackStarted(ctx.payload()))
            ;

    @PostConstruct
    private void init() {
        //playback_timer = timerProvider.getObject("mh.playback.delay", MetricCustomized.builder().tags(List.of("actor", "fsio")).build());
        //transmit_timer = timerProvider.getObject("mh.transmit.delay", MetricCustomized.builder()
//                .tags(List.of("actor", "fsa_io"))
//                .maximumExpected(Duration.ofMinutes(1))
//                .build());
        gaugeProvider.getObject((Supplier<Number>)_wscount::get, "mh.ws.count", MetricCustomized.builder().tags(List.of("actor", "fsa_io")).build());
        executor = executorProvider.apply("wsmsg");
    }

    abstract class AfsIO extends CommandHandler<AfsIO> {
        @Override
        protected WSCommandRegistry<AfsIO> commandRegistry() {
            return cmds;
        }

        public void addLocal(final AFSAddLocalCommand payload, final WebSocket ws) {
            log.info("AfsIO => addLocal: {}", payload);
            final var actor = actorProvider.getObject(new AfsActor.Context() {
                public int localIdx() {
                    return payload.localIdx;
                }
                public String uuid() {
                    return payload.uuid;
                }
                public String sessionId() {
                    return payload.sessionId;
                }
                public String welcome() {
                    return payload.welcome;
                }
                public Consumer<Runnable> runOn() {
                    return runnable -> orderedExecutor.submit(payload.localIdx, runnable);
                }
                public BiFunction<AIReplyVO, Supplier<String>, String> reply2Rms() {
                    return (reply, vars) -> reply2rms(payload.uuid, reply, vars);
                }
                public BiConsumer<String, Object> sendEvent() {
                    return (name,obj)-> WSEventVO.sendEvent(ws, name, obj);
                }
            });
            idx2actor.put(payload.localIdx, actor);
            actor.startTranscription();
        }

        public void removeLocal(final AFSRemoveLocalCommand payload) {
            final var actor = idx2actor.remove(payload.localIdx);
            if (actor != null) {
                actor.close();
            }
            log.info("AfsIO: removeLocal {}", payload.localIdx);
        }

        public void playbackStarted(final AFSPlaybackStarted payload) {
            final var actor = idx2actor.get(payload.localIdx);
            if (actor != null) {
                actor.playbackStarted(payload);
            }
            log.info("AfsIO: playbackStarted {}", payload.localIdx);
        }

        AfsActor actorOf(final int localIdx) {
            return idx2actor.get(localIdx);
        }

        final ConcurrentMap<Integer, AfsActor> idx2actor = new ConcurrentHashMap<>();
    }

    @Override
    public WsHandler build(final String prefix, final WebSocket webSocket, final ClientHandshake handshake) {
        _wscount.incrementAndGet();
        final WsHandler handler = new AfsIO() {
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
                orderedExecutor.submit(localIdx, ()->actorOf(localIdx).transmit(buffer, recvdInMs));
            }

            @Override
            public void onAttached(WebSocket webSocket) {
            }

            @Override
            public void onClose(final WebSocket webSocket) {
                _wscount.decrementAndGet();
                log.info("afs_io onClose {}: ", webSocket);
            }
        };

        webSocket.setAttachment(handler);
        log.info("afs_io connected {}", handshake);
        return handler;
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

    private final ObjectProvider<AfsActor> actorProvider;
    private final ObjectProvider<HandlerUrlBuilder> urlProvider;
    private final Function<String, Executor> executorProvider;
    private final OrderedExecutor orderedExecutor;
    private final ObjectProvider<Timer> timerProvider;
    private final ObjectProvider<Gauge> gaugeProvider;

    private final AtomicInteger _wscount = new AtomicInteger(0);

    private Executor executor;
    private Timer playback_timer;
    private Timer transmit_timer;
}
