package com.yulore.medhub.ws.builder;

import com.yulore.medhub.session.StreamSession;
import com.yulore.medhub.vo.PayloadFileSeekResult;
import com.yulore.medhub.vo.PayloadGetFileLenResult;
import com.yulore.medhub.vo.WSCommandVO;
import com.yulore.medhub.vo.WSEventVO;
import com.yulore.medhub.vo.cmd.*;
import com.yulore.medhub.ws.WSCommandRegistry;
import com.yulore.medhub.ws.actor.StreamActor;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Slf4j
public class BaseStreamBuilder {
    public static final byte[] EMPTY_BYTES = new byte[0];

    Timer open_timer;
    Timer getlen_timer;
    Timer seek_timer;
    Timer read_timer;
    Timer tell_timer;

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

    private void handleGetFileLenCommand(final WebSocket webSocket, final StreamActor actor, final Timer.Sample sample) {
        final long startInMs = System.currentTimeMillis();
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
        log.info("[{}]: stream read => pos:{} count:{}/ss.length:{}", ss.sessionId(), ss.tell(), vo.count, ss.length());
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
            log.info("[{}]: try to read: {} bytes from: {} pos when length: {}, send 0 bytes to rms client", ss.sessionId(), vo.count, ss.tell(), ss.length());
            return;
        }

        readLaterOrNow(startInMs, ss, vo.count, sample);
    }

    private boolean readLaterOrNow(final long startInMs, final StreamSession ss, final int count4read, final Timer.Sample sample) {
        try {
            ss.lock();
            if (ss.needMoreData(count4read)) {
                ss.onDataChange((ignore) -> readLaterOrNow(startInMs, ss, count4read, sample));
                log.info("[{}]: need more data for read: {} bytes, read on append data.", ss.sessionId(), count4read);
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
                log.info("[{}]: stream read => request read count: {}, no_more_data read", ss.sessionId(), count4read);
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
            log.info("[{}]: stream read => request read count: {}, actual read bytes: {}", ss.sessionId(), count4read, readed);
        } catch (IOException ex) {
            log.warn("[{}]: stream read => request read count: {}/length:{}/pos before read:{}, failed",
                    ss.sessionId(), count4read, ss.length(), posBeforeRead, ex);
        } finally {
            ss.unlock();
        }
        return true;
    }

    private void handleFileTellCommand(final WebSocket webSocket, final StreamActor actor, final Timer.Sample sample) {
        final long startInMs = System.currentTimeMillis();
        final StreamSession ss = actor._ss;
        if (ss == null) {
            log.warn("handleFileTellCommand: ss is null, just return 0");
            WSEventVO.sendEvent(webSocket, "FileTellResult", new PayloadFileSeekResult(0));
            return;
        }
        ss.sendEvent(startInMs, "FileTellResult", new PayloadFileSeekResult(ss.tell()));
        sample.stop(tell_timer);
    }

    @Autowired
    private ObjectProvider<ScheduledExecutorService> schedulerProvider;

    final WSCommandRegistry<StreamActor> cmds = new WSCommandRegistry<StreamActor>()
            .register(WSCommandVO.WSCMD_VOID,"GetFileLen",
                    ctx-> handleGetFileLenCommand(ctx.ws(), ctx.actor(), ctx.sample()))
            .register(VOSFileSeek.TYPE,"FileSeek",
                    ctx-> handleFileSeekCommand(ctx.payload(), ctx.ws(), ctx.actor(), ctx.sample()))
            .register(VOSFileRead.TYPE,"FileRead",
                    ctx-> handleFileReadCommand(ctx.payload(), ctx.ws(), ctx.actor(), ctx.sample()))
            .register(WSCommandVO.WSCMD_VOID,"FileTell",
                    ctx-> handleFileTellCommand(ctx.ws(), ctx.actor(), ctx.sample()))
            ;
}
