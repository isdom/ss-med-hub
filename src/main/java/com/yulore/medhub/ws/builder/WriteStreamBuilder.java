package com.yulore.medhub.ws.builder;

import com.aliyun.oss.OSS;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yulore.medhub.session.StreamSession;
import com.yulore.medhub.vo.*;
import com.yulore.medhub.ws.WsHandler;
import com.yulore.medhub.ws.WsHandlerBuilder;
import com.yulore.medhub.ws.actor.StreamActor;
import com.yulore.util.ExceptionUtil;
import com.yulore.util.VarsUtil;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.springframework.beans.factory.ObjectProvider;
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

@Component("writeRms")
@RequiredArgsConstructor
@Slf4j
public class WriteStreamBuilder implements WsHandlerBuilder {
    public static final byte[] EMPTY_BYTES = new byte[0];

    @PostConstruct
    public void start() {
        _ossAccessExecutor = Executors.newFixedThreadPool(NettyRuntime.availableProcessors() * 2,
                new DefaultThreadFactory("ossAccessExecutor"));
        _sessionExecutor = Executors.newFixedThreadPool(NettyRuntime.availableProcessors() * 2,
                new DefaultThreadFactory("sessionExecutor"));
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        _sessionExecutor.shutdownNow();
        _ossAccessExecutor.shutdownNow();
    }

    @Override
    public WsHandler build(final String prefix, final WebSocket webSocket, final ClientHandshake handshake) {
        final StreamActor actor = new StreamActor() {
            @Override
            public void onMessage(final WebSocket webSocket, final String message) {
                try {
                    handleCommand(new ObjectMapper().readValue(message, WSCommandVO.class), webSocket, this);
                } catch (JsonProcessingException ex) {
                    log.error("handleCommand {}: {}, an error occurred when parseAsJson: {}",
                            webSocket.getRemoteSocketAddress(), message, ExceptionUtil.exception2detail(ex));
                }
            }

            @Override
            public void onMessage(final WebSocket webSocket, final ByteBuffer bytes) {
                _sessionExecutor.submit(()-> handleFileWriteCommand(bytes, _ss, webSocket));
            }
        };
        webSocket.setAttachment(actor);
        return actor;
    }

    private void handleCommand(final WSCommandVO cmd, final WebSocket webSocket, final StreamActor actor) {
        if ("OpenStream".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleOpenStreamCommand(cmd, webSocket, actor));
        } else if ("GetFileLen".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleGetFileLenCommand(cmd, webSocket, actor));
        } else if ("FileSeek".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleFileSeekCommand(cmd, webSocket, actor));
        } else if ("FileRead".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleFileReadCommand(cmd, webSocket, actor));
        } else if ("FileTell".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleFileTellCommand(cmd, webSocket, actor));
        }  else {
            log.warn("handleCommand: Unknown Command: {}", cmd);
        }
    }

    Consumer<StreamSession.EventContext> buildSendEvent(final WebSocket webSocket, final int delayInMs) {
        final Consumer<StreamSession.EventContext> performSendEvent = (ctx) -> {
            WSEventVO.sendEvent(webSocket, ctx.name, ctx.payload);
            log.info("sendEvent: {} send => {}, {}, cost {} ms",
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
            log.info("sendData: {} send => {} bytes, cost {} ms",
                    ctx.session, size, System.currentTimeMillis() - ctx.start);
        };
        return delayInMs == 0 ? performSendData : (ctx) -> {
            schedulerProvider.getObject().schedule(() -> performSendData.accept(ctx), delayInMs, TimeUnit.MILLISECONDS);
        };
    }

    private void handleOpenStreamCommand(final WSCommandVO cmd, final WebSocket webSocket, final StreamActor actor) {
        final long startInMs = System.currentTimeMillis();
        final String path = cmd.getPayload().get("path");
        final boolean isWrite = Boolean.parseBoolean(cmd.getPayload().get("is_write"));
        final String sessionId = cmd.getPayload().get("session_id");
        final String contentId = cmd.getPayload().get("content_id");
        final String playIdx = cmd.getPayload().get("playback_idx");

        log.info("[{}]: open read stream => path: {}/is_write: {}/contentId: {}/playIdx: {}",
                sessionId, path, isWrite, contentId, playIdx);
        if (!isWrite) {
            log.warn("[{}]: open write stream with readable, open stream failed!", sessionId);
            webSocket.setAttachment(null); // remove attached actor
            // TODO: define StreamOpened failed event
            WSEventVO.sendEvent(webSocket, "StreamOpened", null);
            return;
        }

        final int delayInMs = VarsUtil.extractValueAsInteger(path, "test_delay", 0);
        final Consumer<StreamSession.EventContext> sendEvent = buildSendEvent(webSocket, delayInMs);
        final Consumer<StreamSession.DataContext> sendData = buildSendData(webSocket, delayInMs);

        final StreamSession _ss = new StreamSession(true, sendEvent, sendData,
                (ctx) -> {
                    final long startUploadInMs = System.currentTimeMillis();
                    _ossAccessExecutor.submit(()->{
                        _ossClient.putObject(ctx.bucketName, ctx.objectName, ctx.content);
                        log.info("[{}]: upload content to oss => bucket:{}/object:{}, cost {} ms",
                                sessionId, ctx.bucketName, ctx.objectName, System.currentTimeMillis() - startUploadInMs);
                    });
                },
                path, sessionId, contentId, playIdx);
        actor._ss = _ss;

        // write mode return StreamOpened event directly
        _ss.sendEvent(startInMs, "StreamOpened", null);
    }

    private void handleGetFileLenCommand(final WSCommandVO cmd, final WebSocket webSocket, final StreamActor actor) {
        final long startInMs = System.currentTimeMillis();
        log.info("get file len:");
        final StreamSession ss = actor._ss;
        if (ss == null) {
            log.warn("handleGetFileLenCommand: ss is null, just return 0");
            WSEventVO.sendEvent(webSocket, "GetFileLenResult", new PayloadGetFileLenResult(0));
            return;
        }
        ss.sendEvent(startInMs, "GetFileLenResult", new PayloadGetFileLenResult(ss.length()));
    }

    private void handleFileSeekCommand(final WSCommandVO cmd, final WebSocket webSocket, final StreamActor actor) {
        final long startInMs = System.currentTimeMillis();
        final int offset = Integer.parseInt(cmd.getPayload().get("offset"));
        final int whence = Integer.parseInt(cmd.getPayload().get("whence"));
        log.info("file seek => offset: {}, whence: {}", offset, whence);
        final StreamSession ss = actor._ss;
        if (ss == null) {
            log.warn("handleFileSeekCommand: ss is null, just return 0");
            WSEventVO.sendEvent(webSocket, "FileSeekResult", new PayloadFileSeekResult(0));
            return;
        }
        int seek_from_start = -1;
        switch (whence) {
            case 0: //SEEK_SET:
                seek_from_start = offset;
                break;
            case 1: //SEEK_CUR:
                seek_from_start = ss.tell() + offset;
                break;
            case 2: //SEEK_END:
                seek_from_start = ss.length() + offset;
                break;
            default:
        }
        int pos = 0;
        // from begin
        if (seek_from_start >= 0) {
            pos = ss.seekFromStart(seek_from_start);
        }
        ss.sendEvent(startInMs,"FileSeekResult", new PayloadFileSeekResult(pos));
    }

    private void handleFileReadCommand(final WSCommandVO cmd, final WebSocket webSocket, final StreamActor actor) {
        final long startInMs = System.currentTimeMillis();
        final int count = Integer.parseInt(cmd.getPayload().get("count"));
        final StreamSession ss = actor._ss;
        if (ss == null) {
            log.warn("handleFileReadCommand: file read => count: {}, and ss is null, send 0 bytes to rms client", count);
            webSocket.send(EMPTY_BYTES);
            return;
        }
        log.info("file read => count: {}/ss.length:{}/ss.tell:{}", count, ss.length(), ss.tell());
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
        if (ss.streaming() && count <= 12 && ss.tell() >= 1024 ) {
            // streaming, and sndfile lib try to jump to eof
            ss.sendData(startInMs, ByteBuffer.wrap(EMPTY_BYTES));
            log.info("try to read: {} bytes from: {} pos when length: {}, send 0 bytes to rms client", count, ss.tell(), ss.length());
            return;
        }

        readLaterOrNow(startInMs, ss, count);
    }

    private static boolean readLaterOrNow(final long startInMs, final StreamSession ss, final int count4read) {
        try {
            ss.lock();
            if (ss.needMoreData(count4read)) {
                ss.onDataChange((ignore) -> readLaterOrNow(startInMs, ss, count4read));
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
                return true;
            }
            // readed > 0
            ss.seekFromStart(ss.tell() + readed);
            if (readed == bytes4read.length) {
                ss.sendData(startInMs, ByteBuffer.wrap(bytes4read));
            } else {
                ss.sendData(startInMs, ByteBuffer.wrap(bytes4read, 0, readed));
            }
            log.info("file read => request read count: {}, actual read bytes: {}", count4read, readed);
        } catch (IOException ex) {
            log.warn("file read => request read count: {}/length:{}/pos before read:{}, failed: {}",
                    count4read, ss.length(), posBeforeRead, ex.toString());
        } finally {
            ss.unlock();
        }
        return true;
    }

    private void handleFileWriteCommand(final ByteBuffer bytes, final StreamSession ss, final WebSocket webSocket) {
        final long startInMs = System.currentTimeMillis();
        final int written = ss.writeToStream(bytes);
        ss.sendEvent(startInMs, "FileWriteResult", new PayloadFileWriteResult(written));
    }

    private void handleFileTellCommand(final WSCommandVO cmd, final WebSocket webSocket, final StreamActor actor) {
        final long startInMs = System.currentTimeMillis();
        final StreamSession ss = actor._ss;
        if (ss == null) {
            log.warn("handleFileTellCommand: ss is null, just return 0");
            WSEventVO.sendEvent(webSocket, "FileTellResult", new PayloadFileSeekResult(0));
            return;
        }
        log.info("file tell: current pos: {}", ss.tell());
        ss.sendEvent(startInMs, "FileTellResult", new PayloadFileSeekResult(ss.tell()));
    }

    private final ObjectProvider<ScheduledExecutorService> schedulerProvider;

    private ExecutorService _sessionExecutor;
    private ExecutorService _ossAccessExecutor;

    private final OSS _ossClient;
}
