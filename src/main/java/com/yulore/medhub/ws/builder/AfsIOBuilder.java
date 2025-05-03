package com.yulore.medhub.ws.builder;

import com.yulore.medhub.service.ASRConsumer;
import com.yulore.medhub.service.ASROperator;
import com.yulore.medhub.service.ASRService;
import com.yulore.medhub.vo.PayloadSentenceBegin;
import com.yulore.medhub.vo.PayloadSentenceEnd;
import com.yulore.medhub.vo.PayloadTranscriptionResultChanged;
import com.yulore.medhub.vo.cmd.AFSAddLocal;
import com.yulore.medhub.vo.cmd.AFSRemoveLocal;
import com.yulore.medhub.ws.WSCommandRegistry;
import com.yulore.medhub.ws.WsHandler;
import com.yulore.medhub.ws.WsHandlerBuilder;
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
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
@Component("afs_io")
@ConditionalOnProperty(prefix = "feature", name = "afs_io", havingValue = "enabled")
public class AfsIOBuilder implements WsHandlerBuilder {

    private final WSCommandRegistry<AfsIO> cmds = new WSCommandRegistry<AfsIO>()
            .register(AFSAddLocal.TYPE,"AddLocal", ctx->ctx.actor().addLocal(ctx.payload()))
            .register(AFSRemoveLocal.TYPE,"RemoveLocal", ctx->ctx.actor().removeLocal(ctx.payload()))
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

        public void addLocal(final AFSAddLocal payload) {
            log.info("AfsIO: addLocal {}", payload.localIdx);
            final var ctx = new AfsCtx();
            idx2ctx.put(payload.localIdx, ctx);
            asrService.startTranscription(new ASRConsumer() {
                @Override
                public void onSentenceBegin(PayloadSentenceBegin payload) {
                }

                @Override
                public void onTranscriptionResultChanged(PayloadTranscriptionResultChanged payload) {
                    log.info("afs_io => onTranscriptionResultChanged: {}", payload);
                }

                @Override
                public void onSentenceEnd(PayloadSentenceEnd payload) {
                    log.info("afs_io => onSentenceEnd: {}", payload);
                }

                @Override
                public void onTranscriberFail() {
                    log.warn("afs_io => onTranscriberFail");
                }
            }).whenComplete((operator, ex) -> {
                if (ex != null) {
                    log.warn("startTranscription failed", ex);
                } else {
                    ctx.asrRef.set(operator);
                }
            });
        }

        public void removeLocal(final AFSRemoveLocal payload) {
            log.info("AfsIO: removeLocal {}", payload.localIdx);
            final AfsCtx ctx = idx2ctx.remove(payload.localIdx);
            if (ctx != null) {
                final ASROperator operator = ctx.asrRef.get();
                if (null != operator) {
                    operator.close();
                }
            }
        }

        final ConcurrentMap<Integer, AfsCtx> idx2ctx = new ConcurrentHashMap<>();
    }

    class AfsCtx {
        final AtomicReference<ASROperator> asrRef = new AtomicReference<>(null);
    }

    @Override
    public WsHandler build(final String prefix, final WebSocket webSocket, final ClientHandshake handshake) {
        _wscount.incrementAndGet();
        final WsHandler handler = new AfsIO() {
            @Override
            public void onMessage(final WebSocket webSocket, final ByteBuffer buffer, final long recvdInMs) {
                final byte[] byte4 = new byte[8];
                buffer.get(byte4, 0, 4);
                // 将小端字节序转换为 int
                final int localIdx =
                        ((byte4[3] & 0xFF) << 24) |
                        ((byte4[2] & 0xFF) << 16) |
                        ((byte4[1] & 0xFF) << 8)  |
                        (byte4[0] & 0xFF);          // 最低有效字节（小端的第一个字节）

                orderedExecutor.submit(localIdx, ()->{
                    final byte[] byte8 = new byte[8];
                    buffer.get(byte8, 0, 8);
                    // 将小端字节序转换为 long
                    final long startInMss =
                            ((byte8[7] & 0xFFL) << 56) |  // 最高有效字节（小端的最后一个字节）
                            ((byte8[5] & 0xFFL) << 40) |
                            ((byte8[6] & 0xFFL) << 48) |
                            ((byte8[4] & 0xFFL) << 32) |
                            ((byte8[3] & 0xFFL) << 24) |
                            ((byte8[2] & 0xFFL) << 16) |
                            ((byte8[1] & 0xFFL) << 8)  |
                            (byte8[0] & 0xFFL);          // 最低有效字节（小端的第一个字节）

                    final var ctx = idx2ctx.get(localIdx);
                    if (ctx != null) {
                        final var operator = ctx.asrRef.get();
                        if (operator != null) {
                            final byte[] pcm = new byte[buffer.remaining()];
                            buffer.get(pcm);
                            operator.transmit(pcm);
                        }
                    }

                    final long nowInMs = System.currentTimeMillis();
                    log.info("afs_io => localIdx: {}/recvd delay: {} ms/process delay: {} ms",
                            localIdx, recvdInMs - startInMss / 1000L, nowInMs - startInMss / 1000L);
                });
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

    private final Function<String, Executor> executorProvider;
    private final ASRService asrService;
    private final OrderedExecutor orderedExecutor;
    private final ObjectProvider<Timer> timerProvider;
    private final ObjectProvider<Gauge> gaugeProvider;

    private final AtomicInteger _wscount = new AtomicInteger(0);

    private Executor executor;
    private Timer playback_timer;
    private Timer transmit_timer;
}
