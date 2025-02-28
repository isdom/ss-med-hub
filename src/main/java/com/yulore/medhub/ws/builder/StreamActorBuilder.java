package com.yulore.medhub.ws.builder;

import com.alibaba.nls.client.protocol.OutputFormatEnum;
import com.alibaba.nls.client.protocol.SampleRateEnum;
import com.aliyun.oss.OSS;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mgnt.utils.StringUnicodeEncoderDecoder;
import com.yulore.bst.*;
import com.yulore.medhub.api.CompositeVO;
import com.yulore.medhub.service.NlsService;
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
import org.springframework.beans.factory.annotation.Autowired;
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

@Component("streamBuilder")
@RequiredArgsConstructor
@Slf4j
public class StreamActorBuilder implements WsHandlerBuilder {
    public static final byte[] EMPTY_BYTES = new byte[0];

    @PostConstruct
    public void start() {
        _ossAccessExecutor = Executors.newFixedThreadPool(NettyRuntime.availableProcessors() * 2,
                new DefaultThreadFactory("ossAccessExecutor"));
        _scheduledExecutor = Executors.newScheduledThreadPool(NettyRuntime.availableProcessors() * 2,
                new DefaultThreadFactory("scheduledExecutor"));
        _sessionExecutor = Executors.newFixedThreadPool(NettyRuntime.availableProcessors() * 2,
                new DefaultThreadFactory("sessionExecutor"));
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        _sessionExecutor.shutdownNow();
        _scheduledExecutor.shutdownNow();
        _ossAccessExecutor.shutdownNow();
    }

    @Override
    public WsHandler build(final String prefix, final WebSocket webSocket, final ClientHandshake handshake) {
        final StreamActor actor = new StreamActor() {
            @Override
            public void onMessage(final WebSocket webSocket, final String message) {
                try {
                    handleCommand(new ObjectMapper().readValue(message, WSCommandVO.class), webSocket);
                } catch (JsonProcessingException ex) {
                    log.error("handleCommand {}: {}, an error occurred when parseAsJson: {}",
                            webSocket.getRemoteSocketAddress(), message, ExceptionUtil.exception2detail(ex));
                }
            }

            @Override
            public void onMessage(final WebSocket webSocket, final ByteBuffer bytes) {
                if (webSocket.getAttachment() instanceof StreamSession session) {
                    _sessionExecutor.submit(()-> handleFileWriteCommand(bytes, session, webSocket));
                    return;
                }
            }
        };
        webSocket.setAttachment(actor);
        return actor;
    }

    private void handleCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        if ("OpenStream".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleOpenStreamCommand(cmd, webSocket));
        } else if ("GetFileLen".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleGetFileLenCommand(cmd, webSocket));
        } else if ("FileSeek".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleFileSeekCommand(cmd, webSocket));
        } else if ("FileRead".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleFileReadCommand(cmd, webSocket));
        } else if ("FileTell".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleFileTellCommand(cmd, webSocket));
        }  else {
            log.warn("handleHubCommand: Unknown Command: {}", cmd);
        }
    }

    Consumer<StreamSession.EventContext> buildSendEvent(final WebSocket webSocket, final int delayInMs) {
        final Consumer<StreamSession.EventContext> performSendEvent = (ctx) -> {
            WSEventVO.sendEvent(webSocket, ctx.name, ctx.payload);
            log.info("sendEvent: {} send => {}, {}, cost {} ms",
                    ctx.session, ctx.name, ctx.payload, System.currentTimeMillis() - ctx.start);
        };
        return delayInMs == 0 ? performSendEvent : (ctx) -> {
            _scheduledExecutor.schedule(() -> performSendEvent.accept(ctx), delayInMs, TimeUnit.MILLISECONDS);
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
            _scheduledExecutor.schedule(() -> performSendData.accept(ctx), delayInMs, TimeUnit.MILLISECONDS);
        };
    }

    private void handleOpenStreamCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        final long startInMs = System.currentTimeMillis();
        final String path = cmd.getPayload().get("path");
        final boolean isWrite = Boolean.parseBoolean(cmd.getPayload().get("is_write"));
        final String sessionId = cmd.getPayload().get("session_id");
        final String contentId = cmd.getPayload().get("content_id");
        final String playIdx = cmd.getPayload().get("playback_idx");

        log.debug("open stream => path: {}/is_write: {}/sessionId: {}/contentId: {}/playIdx: {}",
                path, isWrite, sessionId, contentId, playIdx);

        final int delayInMs = VarsUtil.extractValueAsInteger(path, "test_delay", 0);
        final Consumer<StreamSession.EventContext> sendEvent = buildSendEvent(webSocket, delayInMs);
        final Consumer<StreamSession.DataContext> sendData = buildSendData(webSocket, delayInMs);

        final StreamSession _ss = new StreamSession(isWrite, sendEvent, sendData,
                (ctx) -> {
                    final long startUploadInMs = System.currentTimeMillis();
                    _ossAccessExecutor.submit(()->{
                        _ossClient.putObject(ctx.bucketName, ctx.objectName, ctx.content);
                        log.info("[{}]: upload content to oss => bucket:{}/object:{}, cost {} ms",
                                sessionId, ctx.bucketName, ctx.objectName, System.currentTimeMillis() - startUploadInMs);
                    });
                },
                path, sessionId, contentId, playIdx);
        webSocket.setAttachment(_ss);

        if (!isWrite) {
            final BuildStreamTask bst = getTaskOf(path, false, 8000);
            if (bst == null) {
                webSocket.setAttachment(null); // remove Attached ss
                // TODO: define StreamOpened failed event
                WSEventVO.sendEvent(webSocket, "StreamOpened", null);
                log.warn("OpenStream failed for path: {}/sessionId: {}/contentId: {}/playIdx: {}", path, sessionId, contentId, playIdx);
                return;
            }

            _ss.onDataChange((ss) -> {
                ss.sendEvent(startInMs, "StreamOpened", null);
                return true;
            });
            bst.buildStream(_ss::appendData, (isOK) -> _ss.appendCompleted());
        } else {
            // write mode return StreamOpened event directly
            _ss.sendEvent(startInMs, "StreamOpened", null);
        }
    }

    private BuildStreamTask getTaskOf(final String path, final boolean removeWavHdr, final int sampleRate) {
        try {
            if (path.contains("type=cp")) {
                return new CompositeStreamTask(path, (cvo) -> {
                    final BuildStreamTask bst = cvo2bst(cvo);
                    if (bst != null) {
                        return bst.key() != null ? _scsService.asCache(bst) : bst;
                    }
                    return null;
                }, removeWavHdr);
            } else if (path.contains("type=tts")) {
                final BuildStreamTask bst = new TTSStreamTask(path, nlsService::selectTTSAgent, (synthesizer) -> {
                    synthesizer.setFormat(removeWavHdr ? OutputFormatEnum.PCM : OutputFormatEnum.WAV);
                    synthesizer.setSampleRate(sampleRate);
                });
                return bst.key() != null ? _scsService.asCache(bst) : bst;
            } else if (path.contains("type=cosy")) {
                final BuildStreamTask bst = new CosyStreamTask(path, nlsService::selectCosyAgent, (synthesizer) -> {
                    synthesizer.setFormat(removeWavHdr ? OutputFormatEnum.PCM : OutputFormatEnum.WAV);
                    synthesizer.setSampleRate(sampleRate);
                });
                return bst.key() != null ? _scsService.asCache(bst) : bst;
            } else {
                final BuildStreamTask bst = new OSSStreamTask(path, _ossClient, removeWavHdr);
                return bst.key() != null ? _scsService.asCache(bst) : bst;
            }
        } catch (Exception ex) {
            log.warn("getTaskOf failed: {}", ex.toString());
            return null;
        }
    }

    private BuildStreamTask cvo2bst(final CompositeVO cvo) {
        if (cvo.getBucket() != null && !cvo.getBucket().isEmpty() && cvo.getObject() != null && !cvo.getObject().isEmpty()) {
            log.info("support CVO => OSS Stream: {}", cvo);
            return new OSSStreamTask(
                    "{bucket=" + cvo.bucket + ",cache=" + cvo.cache + ",start=" + cvo.start + ",end=" + cvo.end + "}" + cvo.object,
                    _ossClient, true);
        } else if (cvo.getType() != null && cvo.getType().equals("tts")) {
            log.info("support CVO => TTS Stream: {}", cvo);
            return genTtsStreamTask(cvo);
        } else if (cvo.getType() != null && cvo.getType().equals("cosy")) {
            log.info("support CVO => Cosy Stream: {}", cvo);
            return genCosyStreamTask(cvo);
        } else {
            log.info("not support cvo: {}, skip", cvo);
            return null;
        }
    }

    private BuildStreamTask genCosyStreamTask(final CompositeVO cvo) {
        return new CosyStreamTask(cvo2cosy(cvo), nlsService::selectCosyAgent, (synthesizer) -> {
            //设置返回音频的编码格式
            synthesizer.setFormat(OutputFormatEnum.PCM);
            //设置返回音频的采样率。
            synthesizer.setSampleRate(SampleRateEnum.SAMPLE_RATE_16K);
        });
    }

    private BuildStreamTask genTtsStreamTask(final CompositeVO cvo) {
        return new TTSStreamTask(cvo2tts(cvo), nlsService::selectTTSAgent, (synthesizer) -> {
            //设置返回音频的编码格式
            synthesizer.setFormat(OutputFormatEnum.PCM);
            //设置返回音频的采样率
            synthesizer.setSampleRate(SampleRateEnum.SAMPLE_RATE_16K);
        });
    }

    static private String cvo2tts(final CompositeVO cvo) {
        // {type=tts,voice=xxx,url=ws://172.18.86.131:6789/playback,vars_playback_id=<uuid>,content_id=2088788,vars_start_timestamp=1732028219711854,text='StringUnicodeEncoderDecoder.encodeStringToUnicodeSequence(content)'}
        //          unused.wav
        return String.format("{type=tts,cache=%s,voice=%s,pitch_rate=%s,speech_rate=%s,volume=%s,text=%s}tts.wav",
                cvo.cache,
                cvo.voice,
                cvo.pitch_rate,
                cvo.speech_rate,
                cvo.volume,
                StringUnicodeEncoderDecoder.encodeStringToUnicodeSequence(cvo.text));
    }

    static private String cvo2cosy(final CompositeVO cvo) {
        // eg: {type=cosy,voice=xxx,url=ws://172.18.86.131:6789/cosy,vars_playback_id=<uuid>,content_id=2088788,vars_start_timestamp=1732028219711854,text='StringUnicodeEncoderDecoder.encodeStringToUnicodeSequence(content)'}
        //          unused.wav
        return String.format("{type=cosy,cache=%s,voice=%s,pitch_rate=%s,speech_rate=%s,volume=%s,text=%s}cosy.wav",
                cvo.cache,
                cvo.voice,
                cvo.pitch_rate,
                cvo.speech_rate,
                cvo.volume,
                StringUnicodeEncoderDecoder.encodeStringToUnicodeSequence(cvo.text));
    }

    private void handleGetFileLenCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        final long startInMs = System.currentTimeMillis();
        log.info("get file len:");
        final StreamSession ss = webSocket.getAttachment();
        if (ss == null) {
            log.warn("handleGetFileLenCommand: ss is null, just return 0");
            WSEventVO.sendEvent(webSocket, "GetFileLenResult", new PayloadGetFileLenResult(0));
            return;
        }
        ss.sendEvent(startInMs, "GetFileLenResult", new PayloadGetFileLenResult(ss.length()));
    }

    private void handleFileSeekCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        final long startInMs = System.currentTimeMillis();
        final int offset = Integer.parseInt(cmd.getPayload().get("offset"));
        final int whence = Integer.parseInt(cmd.getPayload().get("whence"));
        log.info("file seek => offset: {}, whence: {}", offset, whence);
        final StreamSession ss = webSocket.getAttachment();
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

    private void handleFileReadCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        final long startInMs = System.currentTimeMillis();
        final int count = Integer.parseInt(cmd.getPayload().get("count"));
        final StreamSession ss = webSocket.getAttachment();
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

    private void handleFileTellCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        final long startInMs = System.currentTimeMillis();
        final StreamSession ss = webSocket.getAttachment();
        if (ss == null) {
            log.warn("handleFileTellCommand: ss is null, just return 0");
            WSEventVO.sendEvent(webSocket, "FileTellResult", new PayloadFileSeekResult(0));
            return;
        }
        log.info("file tell: current pos: {}", ss.tell());
        ss.sendEvent(startInMs, "FileTellResult", new PayloadFileSeekResult(ss.tell()));
    }

    @Autowired
    private StreamCacheService _scsService;

    private ScheduledExecutorService _scheduledExecutor;
    private ExecutorService _sessionExecutor;
    private ExecutorService _ossAccessExecutor;

    private final OSS _ossClient;

    @Autowired
    private NlsService nlsService;
}
