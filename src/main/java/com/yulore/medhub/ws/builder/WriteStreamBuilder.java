package com.yulore.medhub.ws.builder;

import com.aliyun.oss.OSS;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.yulore.medhub.service.CommandExecutor;
import com.yulore.medhub.session.StreamSession;
import com.yulore.medhub.vo.*;
import com.yulore.medhub.vo.cmd.VOSFileRead;
import com.yulore.medhub.vo.cmd.VOSFileSeek;
import com.yulore.medhub.vo.cmd.VOSOpenStream;
import com.yulore.medhub.ws.WsHandler;
import com.yulore.medhub.ws.WsHandlerBuilder;
import com.yulore.medhub.ws.actor.StreamActor;
import com.yulore.util.ExceptionUtil;
import com.yulore.util.VarsUtil;
import io.micrometer.core.instrument.Timer;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Slf4j
@RequiredArgsConstructor
@Component("write_rms")
@ConditionalOnProperty(prefix = "feature", name = "write_rms", havingValue = "enabled")
public class WriteStreamBuilder implements WsHandlerBuilder {
    public static final byte[] EMPTY_BYTES = new byte[0];

    private Timer open_timer;
    private Timer getlen_timer;
    private Timer seek_timer;
    private Timer read_timer;
    private Timer write_timer;
    private Timer tell_timer;

    @PostConstruct
    public void start() {
        open_timer = timerProvider.getObject("rms.wr.duration", "write rms op", new String[]{"op", "open"});
        getlen_timer = timerProvider.getObject("rms.wr.duration", "write rms op", new String[]{"op", "getlen"});
        seek_timer = timerProvider.getObject("rms.wr.duration", "write rms op", new String[]{"op", "seek"});
        read_timer = timerProvider.getObject("rms.wr.duration", "write rms op", new String[]{"op", "read"});
        write_timer = timerProvider.getObject("rms.wr.duration", "write rms op", new String[]{"op", "write"});
        tell_timer = timerProvider.getObject("rms.wr.duration", "write rms op", new String[]{"op", "tell"});

        _ossAccessExecutor = Executors.newFixedThreadPool(NettyRuntime.availableProcessors() * 2,
                new DefaultThreadFactory("ossAccessExecutor"));
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        _ossAccessExecutor.shutdownNow();
    }

    @Override
    public WsHandler build(final String prefix, final WebSocket webSocket, final ClientHandshake handshake) {
        final StreamActor actor = new StreamActor() {
            @Override
            public void onMessage(final WebSocket webSocket, final String message) {
                final Timer.Sample sample = Timer.start();
                cmdExecutorProvider.getObject().submit(()-> {
                    try {
                        handleCommand(WSCommandVO.parse(message, WSCommandVO.WSCMD_VOID), message, webSocket, this, sample);
                    } catch (JsonProcessingException ex) {
                        log.error("handleCommand {}: {}, an error occurred when parseAsJson: {}",
                                webSocket.getRemoteSocketAddress(), message, ExceptionUtil.exception2detail(ex));
                    }
                });
            }

            @Override
            public void onMessage(final WebSocket webSocket, final ByteBuffer bytes) {
                final Timer.Sample sample = Timer.start();
                cmdExecutorProvider.getObject().submit(()-> handleFileWriteCommand(bytes, _ss, webSocket, sample));
            }
        };
        webSocket.setAttachment(actor);
        return actor;
    }

    private void handleCommand(final WSCommandVO<Void> cmd,
                               final String message,
                               final WebSocket webSocket,
                               final StreamActor actor,
                               final Timer.Sample sample) throws JsonProcessingException {
        if ("OpenStream".equals(cmd.getHeader().get("name"))) {
            handleOpenStreamCommand(VOSOpenStream.of(message), webSocket, actor, sample);
        } else if ("GetFileLen".equals(cmd.getHeader().get("name"))) {
            handleGetFileLenCommand(webSocket, actor, sample);
        } else if ("FileSeek".equals(cmd.getHeader().get("name"))) {
            handleFileSeekCommand(VOSFileSeek.of(message), webSocket, actor, sample);
        } else if ("FileRead".equals(cmd.getHeader().get("name"))) {
            handleFileReadCommand(VOSFileRead.of(message), webSocket, actor, sample);
        } else if ("FileTell".equals(cmd.getHeader().get("name"))) {
            handleFileTellCommand(webSocket, actor, sample);
        } else {
            log.warn("handleCommand: Unknown Command: {}", cmd);
        }
    }

    Consumer<StreamSession.EventContext> buildSendEvent(final WebSocket webSocket, final int delayInMs) {
        final Consumer<StreamSession.EventContext> performSendEvent = (ctx) -> {
            WSEventVO.sendEvent(webSocket, ctx.name, ctx.payload);
            log.debug("sendEvent: {} send => {}, {}, cost {} ms",
                    ctx.session, ctx.name, ctx.payload, System.currentTimeMillis() - ctx.start);
        };
        return delayInMs == 0 ? performSendEvent : (ctx) -> {
            schedulerProvider.getObject().schedule(() -> performSendEvent.accept(ctx), delayInMs, TimeUnit.MILLISECONDS);
        };
    }

    Consumer<StreamSession.DataContext> buildSendData(final WebSocket webSocket, final int delayInMs) {
        Consumer<StreamSession.DataContext> performSendData = (ctx) -> {
            final int size = ctx.data.remaining();
            webSocket.send(ctx.data);
            log.debug("sendData: {} send => {} bytes, cost {} ms",
                    ctx.session, size, System.currentTimeMillis() - ctx.start);
        };
        return delayInMs == 0 ? performSendData : (ctx) -> {
            schedulerProvider.getObject().schedule(() -> performSendData.accept(ctx), delayInMs, TimeUnit.MILLISECONDS);
        };
    }

    private void handleOpenStreamCommand(final VOSOpenStream vo, final WebSocket webSocket, final StreamActor actor, final Timer.Sample sample) {
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
                    _ossAccessExecutor.submit(()->{
                        try {
                            _ossProvider.getObject().putObject(ctx.bucketName, ctx.objectName, ctx.content);
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

    private void handleGetFileLenCommand(final WebSocket webSocket, final StreamActor actor, final Timer.Sample sample) {
        final long startInMs = System.currentTimeMillis();
        log.info("get file len:");
        final StreamSession ss = actor._ss;
        if (ss == null) {
            log.warn("handleGetFileLenCommand: ss is null, just return 0");
            WSEventVO.sendEvent(webSocket, "GetFileLenResult", new PayloadGetFileLenResult(0));
            return;
        }
        ss.sendEvent(startInMs, "GetFileLenResult", new PayloadGetFileLenResult(ss.length()));
        sample.stop(getlen_timer);
    }

    private void handleFileSeekCommand(final VOSFileSeek vo, final WebSocket webSocket, final StreamActor actor, final Timer.Sample sample) {
        final long startInMs = System.currentTimeMillis();
        log.info("file seek => offset: {}, whence: {}", vo.offset, vo.whence);
        final StreamSession ss = actor._ss;
        if (ss == null) {
            log.warn("handleFileSeekCommand: ss is null, just return 0");
            WSEventVO.sendEvent(webSocket, "FileSeekResult", new PayloadFileSeekResult(0));
            return;
        }
        int seek_from_start = -1;
        switch (vo.whence) {
            case 0: //SEEK_SET:
                seek_from_start = vo.offset;
                break;
            case 1: //SEEK_CUR:
                seek_from_start = ss.tell() + vo.offset;
                break;
            case 2: //SEEK_END:
                seek_from_start = ss.length() + vo.offset;
                break;
            default:
        }
        int pos = 0;
        // from begin
        if (seek_from_start >= 0) {
            pos = ss.seekFromStart(seek_from_start);
        }
        ss.sendEvent(startInMs,"FileSeekResult", new PayloadFileSeekResult(pos));
        sample.stop(seek_timer);
    }

    private void handleFileReadCommand(final VOSFileRead vo, final WebSocket webSocket, final StreamActor actor, final Timer.Sample sample) {
        final long startInMs = System.currentTimeMillis();
        final StreamSession ss = actor._ss;
        if (ss == null) {
            log.warn("handleFileReadCommand: file read => count: {}, and ss is null, send 0 bytes to rms client", vo.count);
            webSocket.send(EMPTY_BYTES);
            return;
        }
        log.info("file read => count: {}/ss.length:{}/ss.tell:{}", vo.count, ss.length(), ss.tell());
        /*
        mod_sndrms read .wav file's op seq:
        =======================================================
        file read => count: 12/ss.length:2147483647/ss.tell:0
        file read => count: 4/ss.length:2147483647/ss.tell:12
        file read => count: 4/ss.length:2147483647/ss.tell:16
        file read => count: 2/ss.length:2147483647/ss.tell:20
        file read => count: 2/ss.length:2147483647/ss.tell:22
        file read => count: 4/ss.length:2147483647/ss.tell:24
        file read => count: 4/ss.length:2147483647/ss.tell:28
        file read => count: 2/ss.length:2147483647/ss.tell:32
        file read => count: 2/ss.length:2147483647/ss.tell:34
        file read => count: 4/ss.length:2147483647/ss.tell:36
        file read => count: 4/ss.length:2147483647/ss.tell:40
        file read => count: 4/ss.length:2147483647/ss.tell:198444  <-- streaming, and sndfile lib try to jump to eof
        file read => count: 4/ss.length:94042/ss.tell:198444
        file read => count: 4/ss.length:94042/ss.tell:44
        file read => count: 32768/ss.length:94042/ss.tell:44
        file read => count: 32768/ss.length:94042/ss.tell:32812
        file read => count: 32768/ss.length:94042/ss.tell:65580
        file read => count: 32768/ss.length:94042/ss.tell:94042
        =======================================================
         */
        if (ss.streaming() && vo.count <= 12 && ss.tell() >= 1024 ) {
            // streaming, and sndfile lib try to jump to eof
            ss.sendData(startInMs, ByteBuffer.wrap(EMPTY_BYTES));
            sample.stop(read_timer);
            log.info("try to read: {} bytes from: {} pos when length: {}, send 0 bytes to rms client", vo.count, ss.tell(), ss.length());
            return;
        }

        readLaterOrNow(startInMs, ss, vo.count, sample);
    }

    private boolean readLaterOrNow(final long startInMs, final StreamSession ss, final int count4read, final Timer.Sample sample) {
        try {
            ss.lock();
            if (ss.needMoreData(count4read)) {
                ss.onDataChange((ignore) -> readLaterOrNow(startInMs, ss, count4read, sample));
                log.info("need more data for read: {} bytes, read on append data.", count4read);
                return false;
            }
        } finally {
            ss.unlock();
        }
        final int posBeforeRead = ss.tell();
        try {
            ss.lock();
            final byte[] bytes4read = new byte[count4read];
            final int readed = ss.genInputStream().read(bytes4read);
            if (readed <= 0) {
                log.info("file read => request read count: {}, no_more_data read", count4read);
                ss.sendData(startInMs, ByteBuffer.wrap(EMPTY_BYTES));
                sample.stop(read_timer);
                return true;
            }
            // readed > 0
            ss.seekFromStart(ss.tell() + readed);
            if (readed == bytes4read.length) {
                ss.sendData(startInMs, ByteBuffer.wrap(bytes4read));
            } else {
                ss.sendData(startInMs, ByteBuffer.wrap(bytes4read, 0, readed));
            }
            sample.stop(read_timer);
            log.info("file read => request read count: {}, actual read bytes: {}", count4read, readed);
        } catch (IOException ex) {
            log.warn("file read => request read count: {}/length:{}/pos before read:{}, failed: {}",
                    count4read, ss.length(), posBeforeRead, ex.toString());
        } finally {
            ss.unlock();
        }
        return true;
    }

    private void handleFileWriteCommand(final ByteBuffer bytes, final StreamSession ss, final WebSocket webSocket, final Timer.Sample sample) {
        final long startInMs = System.currentTimeMillis();
        final int written = ss.writeToStream(bytes);
        ss.sendEvent(startInMs, "FileWriteResult", new PayloadFileWriteResult(written));
        sample.stop(write_timer);
    }

    private void handleFileTellCommand(final WebSocket webSocket, final StreamActor actor, final Timer.Sample sample) {
        final long startInMs = System.currentTimeMillis();
        final StreamSession ss = actor._ss;
        if (ss == null) {
            log.warn("handleFileTellCommand: ss is null, just return 0");
            WSEventVO.sendEvent(webSocket, "FileTellResult", new PayloadFileSeekResult(0));
            return;
        }
        log.info("file tell: current pos: {}", ss.tell());
        ss.sendEvent(startInMs, "FileTellResult", new PayloadFileSeekResult(ss.tell()));
        sample.stop(tell_timer);
    }

    private final ObjectProvider<ScheduledExecutorService> schedulerProvider;
    private final ObjectProvider<CommandExecutor> cmdExecutorProvider;
    private final ObjectProvider<OSS> _ossProvider;
    private final ObjectProvider<Timer> timerProvider;

    private ExecutorService _ossAccessExecutor;
}
