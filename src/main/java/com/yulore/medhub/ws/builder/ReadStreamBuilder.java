package com.yulore.medhub.ws.builder;

import com.yulore.bst.*;
import com.yulore.medhub.service.BSTService;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
@Component("read_rms")
@ConditionalOnProperty(prefix = "feature", name = "read_rms", havingValue = "enabled")
public class ReadStreamBuilder extends BaseStreamBuilder implements WsHandlerBuilder {
    @PostConstruct
    private void init() {
        open_timer = timerProvider.getObject("rms.ro.duration", MetricCustomized.builder().tags(List.of("op", "open")).build());
        getlen_timer = timerProvider.getObject("rms.ro.duration", MetricCustomized.builder().tags(List.of("op", "getlen")).build());
        seek_timer = timerProvider.getObject("rms.ro.duration", MetricCustomized.builder().tags(List.of("op", "seek")).build());
        read_timer = timerProvider.getObject("rms.ro.duration", MetricCustomized.builder().tags(List.of("op", "read")).build());
        tell_timer = timerProvider.getObject("rms.ro.duration", MetricCustomized.builder().tags(List.of("op", "tell")).build());
        gaugeProvider.getObject((Supplier<Number>)_wscount::get, "mh.ws.count", MetricCustomized.builder().tags(List.of("actor", "rrms")).build());

        cmds.register(VOSOpenStream.TYPE, "OpenStream",
                ctx->handleOpenStreamCommand(ctx.payload(), ctx.ws(), ctx.actor(), ctx.sample()));
    }

    @Override
    public WsHandler build(final String prefix, final WebSocket webSocket, final ClientHandshake handshake) {

        final Executor wsmsg = executorProvider.apply("wsmsg");
        final StreamActor actor = new StreamActor() {

            @Override
            protected WSCommandRegistry<StreamActor> commandRegistry() {
                return cmds;
            }

            @Override
            public void onMessage(final WebSocket webSocket, final String message, final Timer.Sample sample) {
                wsmsg.execute(()->super.onMessage(webSocket, message, sample));
            }

            @Override
            public void onMessage(final WebSocket webSocket, final ByteBuffer bytes, final long timestampInMs) {
                log.error("[{}]: Unsupported write command for readonly stream", _ss.sessionId());
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

        log.info("[{}]: open readonly stream => path: {}/is_write: {}/contentId: {}/playIdx: {}",
                vo.session_id, vo.path, vo.is_write, vo.content_id, vo.playback_idx);
        if (vo.is_write) {
            log.warn("[{}]: open read stream with writable, open stream failed!", vo.session_id);
            webSocket.setAttachment(null); // remove attached actor
            // TODO: define StreamOpened failed event
            WSEventVO.sendEvent(webSocket, "StreamOpened", null);
            return;
        }

        final int delayInMs = VarsUtil.extractValueAsInteger(vo.path, "test_delay", 0);
        final Consumer<StreamSession.EventContext> sendEvent = buildSendEvent(webSocket, delayInMs);
        final Consumer<StreamSession.DataContext> sendData = buildSendData(webSocket, delayInMs);

        final StreamSession _ss = new StreamSession(false, sendEvent, sendData,
                (ctx) -> log.warn("[{}]: Unsupported Operation: upload content to oss => bucket:{}/object:{}",
                                vo.session_id, ctx.bucketName, ctx.objectName),
                vo.path, vo.session_id, vo.content_id, vo.playback_idx);
        actor._ss = _ss;

        final BuildStreamTask bst = bstService.getTaskOf(vo.path, false, 8000);
        if (bst == null) {
            webSocket.setAttachment(null); // remove attached actor
            // TODO: define StreamOpened failed event
            WSEventVO.sendEvent(webSocket, "StreamOpened", null);
            log.warn("OpenStream failed for path: {}/sessionId: {}/contentId: {}/playIdx: {}", vo.path, vo.session_id, vo.content_id, vo.playback_idx);
            return;
        }

        _ss.onDataChange((ss) -> {
            ss.sendEvent(startInMs, "StreamOpened", null);
            sample.stop(open_timer);
            return true;
        });
        bst.buildStream(_ss::appendData, (isOK) -> _ss.appendCompleted());
    }

    private final BSTService bstService;
    private final Function<String, Executor> executorProvider;
    private final ObjectProvider<Timer> timerProvider;
    private final ObjectProvider<Gauge> gaugeProvider;

    private final AtomicInteger _wscount = new AtomicInteger(0);
}
