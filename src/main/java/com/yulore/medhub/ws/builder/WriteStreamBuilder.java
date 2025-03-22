package com.yulore.medhub.ws.builder;

import com.aliyun.oss.OSS;
import com.yulore.medhub.session.StreamSession;
import com.yulore.medhub.vo.*;
import com.yulore.medhub.vo.cmd.VOSOpenStream;
import com.yulore.medhub.ws.WSCommandRegistry;
import com.yulore.medhub.ws.WsHandler;
import com.yulore.medhub.ws.WsHandlerBuilder;
import com.yulore.medhub.ws.actor.StreamActor;
import com.yulore.metric.MetricCustomized;
import com.yulore.util.VarsUtil;
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
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
@Component("write_rms")
@ConditionalOnProperty(prefix = "feature", name = "write_rms", havingValue = "enabled")
public class WriteStreamBuilder extends BaseStreamBuilder implements WsHandlerBuilder {
    private Timer write_timer;
    private Timer oss_timer;

    @PostConstruct
    private void init() {
        open_timer = timerProvider.getObject("rms.wr.duration", MetricCustomized.builder().tags(List.of("op", "open")).build());
        getlen_timer = timerProvider.getObject("rms.wr.duration", MetricCustomized.builder().tags(List.of("op", "getlen")).build());
        seek_timer = timerProvider.getObject("rms.wr.duration", MetricCustomized.builder().tags(List.of("op", "seek")).build());
        read_timer = timerProvider.getObject("rms.wr.duration", MetricCustomized.builder().tags(List.of("op", "read")).build());
        write_timer = timerProvider.getObject("rms.wr.duration", MetricCustomized.builder().tags(List.of("op", "write")).build());
        tell_timer = timerProvider.getObject("rms.wr.duration", MetricCustomized.builder().tags(List.of("op", "tell")).build());
        oss_timer = timerProvider.getObject("oss.upload.duration", MetricCustomized.builder().tags(List.of("actor", "wrms")).build());
        gaugeProvider.getObject((Supplier<Number>)_wscount::get, "mh.ws.count", MetricCustomized.builder().tags(List.of("actor", "wrms")).build());

        ossExecutor = executorProvider.apply("longTimeExecutor");
        executor = executorProvider.apply("wsmsg");

        cmds.register(VOSOpenStream.TYPE, "OpenStream",
                ctx->handleOpenStreamCommand(ctx.payload(), ctx.ws(), ctx.actor(), ctx.sample()));
    }

    @Override
    public WsHandler build(final String prefix, final WebSocket webSocket, final ClientHandshake handshake) {
        final StreamActor actor = new StreamActor() {

            @Override
            protected WSCommandRegistry<StreamActor> commandRegistry() {
                return cmds;
            }

            @Override
            public void onMessage(final WebSocket webSocket, final ByteBuffer bytes, final long timestampInMs) {
                executor.execute(()-> handleFileWriteCommand(bytes, _ss, timestampInMs));
            }

            @Override
            public void onClose(final WebSocket webSocket) {
                _wscount.decrementAndGet();
                super.onClose(webSocket);
            }
        };
        _wscount.incrementAndGet();
        webSocket.setAttachment(actor);
        return actor;
    }

    void handleOpenStreamCommand(final VOSOpenStream vo, final WebSocket webSocket, final StreamActor actor, final Timer.Sample sample) {
        final long startInMs = System.currentTimeMillis();

        log.info("[{}]: open write stream => path: {}/is_write: {}/contentId: {}/playIdx: {}",
                vo.session_id, vo.path, vo.is_write, vo.content_id, vo.playback_idx);
        if (!vo.is_write) {
            log.warn("[{}]: open write stream with readonly, open stream failed!", vo.session_id);
            webSocket.setAttachment(null); // remove attached actor
            // TODO: define StreamOpened failed event
            WSEventVO.sendEvent(webSocket, "StreamOpened", null);
            return;
        }

        final int delayInMs = VarsUtil.extractValueAsInteger(vo.path, "test_delay", 0);
        final Consumer<StreamSession.EventContext> sendEvent = buildSendEvent(webSocket, delayInMs);
        final Consumer<StreamSession.DataContext> sendData = buildSendData(webSocket, delayInMs);

        final StreamSession _ss = new StreamSession(true, sendEvent, sendData,
                (ctx) -> {
                    final long startUploadInMs = System.currentTimeMillis();
                    final Timer.Sample oss_sample = Timer.start();
                    ossExecutor.execute(()->{
                        try {
                            ossProvider.getObject().putObject(ctx.bucketName, ctx.objectName, ctx.content);
                            oss_sample.stop(oss_timer);
                            log.info("[{}]: upload content to oss => bucket:{}/object:{}, cost {} ms",
                                    vo.session_id, ctx.bucketName, ctx.objectName, System.currentTimeMillis() - startUploadInMs);
                        } catch (Exception ex) {
                            log.warn("[{}]: upload content to oss => bucket:{}/object:{} failed", vo.session_id, ctx.bucketName, ctx.objectName, ex);
                        }
                    });
                },
                vo.path, vo.session_id, vo.content_id, vo.playback_idx);
        actor._ss = _ss;

        // write mode return StreamOpened event directly
        _ss.sendEvent(startInMs, "StreamOpened", null);
        sample.stop(open_timer);
    }

    private void handleFileWriteCommand(final ByteBuffer bytes, final StreamSession ss, final long timestampInMs) {
        final int written = ss.writeToStream(bytes);
        ss.sendEvent(timestampInMs, "FileWriteResult", new PayloadFileWriteResult(written));
        write_timer.record(System.currentTimeMillis() - timestampInMs, TimeUnit.MILLISECONDS);
    }

    private final Function<String, Executor> executorProvider;
    private final ObjectProvider<OSS> ossProvider;
    private final ObjectProvider<Timer> timerProvider;
    private final ObjectProvider<Gauge> gaugeProvider;

    private Executor ossExecutor;
    private Executor executor;

    private final AtomicInteger _wscount = new AtomicInteger(0);
}
