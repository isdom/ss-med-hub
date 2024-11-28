package com.yulore.medhub;

import com.alibaba.nls.client.protocol.InputFormatEnum;
import com.alibaba.nls.client.protocol.NlsClient;
import com.alibaba.nls.client.protocol.SampleRateEnum;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriber;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriberListener;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriberResponse;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.OSSObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yulore.medhub.nls.CosyAgent;
import com.yulore.medhub.stream.*;
import com.yulore.medhub.stream.StreamCacheService;
import com.yulore.medhub.nls.ASRAgent;
import com.yulore.medhub.nls.TTSAgent;
import com.yulore.medhub.nls.TTSTask;
import com.yulore.medhub.session.MediaSession;
import com.yulore.medhub.session.StreamSession;
import com.yulore.medhub.task.PlayPCMTask;
import com.yulore.medhub.task.SampleInfo;
import com.yulore.medhub.vo.*;
import com.yulore.util.ByteArrayListInputStream;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;
import org.java_websocket.exceptions.WebsocketNotConnectedException;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.UnsupportedAudioFileException;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@Slf4j
@Component
public class HubMain {
    public static final byte[] EMPTY_BYTES = new byte[0];
    @Value("${ws_server.host}")
    private String _ws_host;

    @Value("${ws_server.port}")
    private int _ws_port;

    // in seconds
    //@Value("${ws_server.heartbeat}")
    //private int _ws_heartbeat;

    @Value("${nls.url}")
    private String _nls_url;

    @Value("#{${nls.asr}}")
    private Map<String,String> _all_asr;

    @Value("#{${nls.tts}}")
    private Map<String,String> _all_tts;

    @Value("#{${nls.cosy}}")
    private Map<String,String> _all_cosy;

    @Value("${test.enable_delay}")
    private boolean _test_enable_delay;

    @Value("${test.delay_ms}")
    private long _test_delay_ms;

    @Value("${test.enable_disconnect}")
    private boolean _test_enable_disconnect;

    @Value("${test.disconnect_probability}")
    private float _test_disconnect_probability;

    @Value("${session.check_idle_interval_ms}")
    private long _check_idle_interval_ms;

    @Value("${session.match_resource}")
    private String _match_resource;

    final List<ASRAgent> _asrAgents = new ArrayList<>();
    final List<TTSAgent> _ttsAgents = new ArrayList<>();
    final List<CosyAgent> _cosyAgents = new ArrayList<>();

    private ScheduledExecutorService _nlsAuthExecutor;
    
    WebSocketServer _wsServer;

    private NlsClient _nlsClient;

    private ExecutorService _sessionExecutor;

    private ScheduledExecutorService _scheduledExecutor;

    @Value("${oss.endpoint}")
    private String _oss_endpoint;

    @Value("${oss.access_key_id}")
    private String _oss_access_key_id;

    @Value("${oss.access_key_secret}")
    private String _oss_access_key_secret;

    @Value("${oss.bucket}")
    private String _oss_bucket;

    @Value("${oss.match_prefix}")
    private String _oss_match_prefix;

    private OSS _ossClient;

    private ExecutorService _ossDownloader;

    private final AtomicInteger _currentWSConnection = new AtomicInteger(0);

    @Autowired
    private StreamCacheService _scsService;

    @PostConstruct
    public void start() {
        //创建NlsClient实例应用全局创建一个即可。生命周期可和整个应用保持一致，默认服务地址为阿里云线上服务地址。
        _nlsClient = new NlsClient(_nls_url, "invalid_token");
        _ossClient = new OSSClientBuilder().build(_oss_endpoint, _oss_access_key_id, _oss_access_key_secret);

        initNlsAgents(_nlsClient);

        _ossDownloader = Executors.newFixedThreadPool(NettyRuntime.availableProcessors() * 2, new DefaultThreadFactory("ossDownloader"));
        _sessionExecutor = Executors.newFixedThreadPool(NettyRuntime.availableProcessors() * 2, new DefaultThreadFactory("sessionExecutor"));
        _scheduledExecutor = Executors.newScheduledThreadPool(NettyRuntime.availableProcessors() * 2, new DefaultThreadFactory("scheduledExecutor"));

        _wsServer = new WebSocketServer(new InetSocketAddress(_ws_host, _ws_port), NettyRuntime.availableProcessors() * 2) {
                    @Override
                    public void onOpen(final WebSocket webSocket, final ClientHandshake clientHandshake) {
                        log.info("wscount/{}: new connection from {}, to: {}, uri path: {}",
                                _currentWSConnection.incrementAndGet(),
                                webSocket.getRemoteSocketAddress(),
                                webSocket.getLocalSocketAddress(),
                                clientHandshake.getResourceDescriptor());
                        if (_match_resource.equals(clientHandshake.getResourceDescriptor())) {
                            // init session attach with webSocket
                            final String sessionId = clientHandshake.getFieldValue("x-sessionid");
                            final MediaSession session = new MediaSession(sessionId, _test_enable_delay, _test_delay_ms,
                                    _test_enable_disconnect, _test_disconnect_probability, ()->webSocket.close(1006, "test_disconnect"));
                            webSocket.setAttachment(session);
                            session.scheduleCheckIdle(_scheduledExecutor, _check_idle_interval_ms,
                                    ()->HubEventVO.<Void>sendEvent(webSocket, "CheckIdle", null)); // 5 seconds
                            log.info("ws path match: {}, using ws as MediaSession {}", _match_resource, sessionId);
                        } else {
                            log.info("ws path {} !NOT! match: {}, NOT MediaSession: {}",
                                    clientHandshake.getResourceDescriptor(), _match_resource, webSocket.getRemoteSocketAddress());
                        }
                    }

                    @Override
                    public void onClose(final WebSocket webSocket, final int code, final String reason, final boolean remote) {
                        final Object attachment = webSocket.getAttachment();
                        if (attachment instanceof MediaSession session) {
                            log.info("wscount/{}: closed {} with exit code {} additional info: {}, session: {}",
                                    _currentWSConnection.decrementAndGet(),
                                    webSocket.getRemoteSocketAddress(), code, reason, session.get_sessionId());
                            stopAndCloseTranscriber(webSocket);
                            session.close();
                        } else {
                            log.info("wscount/{}: closed {} with exit code {} additional info: {}",
                                    _currentWSConnection.decrementAndGet(),
                                    webSocket.getRemoteSocketAddress(), code, reason);
                        }
                    }

                    @Override
                    public void onMessage(final WebSocket webSocket, final String message) {
                        log.info("received text message from {}: {}", webSocket.getRemoteSocketAddress(), message);
                        try {
                            handleHubCommand(new ObjectMapper().readValue(message, HubCommandVO.class), webSocket);
                        } catch (JsonProcessingException ex) {
                            log.error("handleASRCommand {}: {}, an error occurred when parseAsJson: {}",
                                    webSocket.getRemoteSocketAddress(), message, ex.toString());
                        }
                    }

                    @Override
                    public void onMessage(final WebSocket webSocket, final ByteBuffer bytes) {
                        // log.info("received binary message from {}: isDirect: {}", webSocket.getRemoteSocketAddress(), bytes.isDirect());
                        handleASRData(bytes, webSocket);
                    }

                    @Override
                    public void onError(WebSocket webSocket, Exception ex) {
                        log.info("an error occurred on connection {}:{}", webSocket.getRemoteSocketAddress(), ex.toString());

                    }

                    @Override
                    public void onStart() {
                        log.info("server started successfully");
                    }
                };
        // _wsServer.setConnectionLostTimeout(_ws_heartbeat);
        _wsServer.start();
    }

    private void initNlsAgents(final NlsClient client) {
        _asrAgents.clear();
        if (_all_asr != null) {
            for (Map.Entry<String, String> entry : _all_asr.entrySet()) {
                log.info("asr: {} / {}", entry.getKey(), entry.getValue());
                final String[] values = entry.getValue().split(" ");
                log.info("asr values detail: {}", Arrays.toString(values));
                final ASRAgent agent = ASRAgent.parse(entry.getKey(), entry.getValue());
                if (null == agent) {
                    log.warn("asr init failed by: {}/{}", entry.getKey(), entry.getValue());
                } else {
                    agent.setClient(client);
                    _asrAgents.add(agent);
                }
            }
        }
        log.info("asr agent init, count:{}", _asrAgents.size());

        if (_all_tts != null) {
            for (Map.Entry<String, String> entry : _all_tts.entrySet()) {
                log.info("tts: {} / {}", entry.getKey(), entry.getValue());
                final String[] values = entry.getValue().split(" ");
                log.info("tts values detail: {}", Arrays.toString(values));
                final TTSAgent agent = TTSAgent.parse(entry.getKey(), entry.getValue());
                if (null == agent) {
                    log.warn("tts init failed by: {}/{}", entry.getKey(), entry.getValue());
                } else {
                    agent.setClient(client);
                    _ttsAgents.add(agent);
                }
            }
        }
        log.info("tts agent init, count:{}", _ttsAgents.size());

        if (_all_cosy != null) {
            for (Map.Entry<String, String> entry : _all_cosy.entrySet()) {
                log.info("cosy: {} / {}", entry.getKey(), entry.getValue());
                final String[] values = entry.getValue().split(" ");
                log.info("cosy values detail: {}", Arrays.toString(values));
                final CosyAgent agent = CosyAgent.parse(entry.getKey(), entry.getValue());
                if (null == agent) {
                    log.warn("cosy init failed by: {}/{}", entry.getKey(), entry.getValue());
                } else {
                    agent.setClient(client);
                    _cosyAgents.add(agent);
                }
            }
        }
        log.info("cosy agent init, count:{}", _cosyAgents.size());

        _nlsAuthExecutor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("nlsAuthExecutor"));
        _nlsAuthExecutor.scheduleAtFixedRate(this::checkAndUpdateNlsToken, 0, 10, TimeUnit.SECONDS);
    }

    private ASRAgent selectASRAgent() {
        for (ASRAgent agent : _asrAgents) {
            final ASRAgent selected = agent.checkAndSelectIfhasIdle();
            if (null != selected) {
                log.info("select asr({}): {}/{}", agent.getName(), agent.get_connectingOrConnectedCount().get(), agent.getLimit());
                return selected;
            }
        }
        throw new RuntimeException("all asr agent has full");
    }

    private TTSAgent selectTTSAgent() {
        for (TTSAgent agent : _ttsAgents) {
            final TTSAgent selected = agent.checkAndSelectIfhasIdle();
            if (null != selected) {
                log.info("select tts({}): {}/{}", agent.getName(), agent.get_connectingOrConnectedCount().get(), agent.getLimit());
                return selected;
            }
        }
        throw new RuntimeException("all tts agent has full");
    }

    private CosyAgent selectCosyAgent() {
        for (CosyAgent agent : _cosyAgents) {
            final CosyAgent selected = agent.checkAndSelectIfhasIdle();
            if (null != selected) {
                log.info("select cosy({}): {}/{}", agent.getName(), agent.get_connectingOrConnectedCount().get(), agent.getLimit());
                return selected;
            }
        }
        throw new RuntimeException("all cosy agent has full");
    }

    private void checkAndUpdateNlsToken() {
        for (ASRAgent agent : _asrAgents) {
            agent.checkAndUpdateAccessToken();
        }
        for (TTSAgent agent : _ttsAgents) {
            agent.checkAndUpdateAccessToken();
        }
        for (CosyAgent agent : _cosyAgents) {
            agent.checkAndUpdateAccessToken();
        }
    }

    private void handleASRData(final ByteBuffer bytes, final WebSocket webSocket) {
        final MediaSession session = webSocket.getAttachment();
        if (session == null) {
            log.error("handleASRData: {} without Session, abort", webSocket.getRemoteSocketAddress());
            return;
        }
        if (session.transmit(bytes)) {
            // transmit success
            if ((session.transmitCount() % 50) == 0) {
                log.debug("{}: transmit 50 times.", session.get_sessionId());
            }
        }
    }

    private void handleHubCommand(final HubCommandVO cmd, final WebSocket webSocket) {
        if ("StartTranscription".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleStartTranscriptionCommand(cmd, webSocket));
        } else if ("StopTranscription".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleStopTranscriptionCommand(cmd, webSocket));
        } else if ("Playback".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handlePlaybackCommand(cmd, webSocket));
        } else if ("PlayTTS".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handlePlayTTSCommand(cmd, webSocket));
        } else if ("StopPlayback".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleStopPlaybackCommand(cmd, webSocket));
        } else if ("OpenStream".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleOpenStreamCommand(cmd, webSocket));
        } else if ("GetFileLen".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleGetFileLenCommand(cmd, webSocket));
        } else if ("FileSeek".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleFileSeekCommand(cmd, webSocket));
        } else if ("FileRead".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleFileReadCommand(cmd, webSocket));
        } else if ("FileTell".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleFileTellCommand(cmd, webSocket));
        } else {
            log.warn("handleHubCommand: Unknown Command: {}", cmd);
        }
    }

    private void handleOpenStreamCommand(final HubCommandVO cmd, final WebSocket webSocket) {
        final long startInMs = System.currentTimeMillis();
        final String path = cmd.getPayload().get("path");
        final String sessionId = cmd.getPayload().get("session_id");
        final String contentId = cmd.getPayload().get("content_id");
        final String playIdx = cmd.getPayload().get("playback_idx");

        log.debug("open stream => path: {}/sessionId: {}/contentId: {}/playIdx: {}", path, sessionId, contentId, playIdx);

        final BuildStreamTask bst = getTaskOf(path);
        if (bst == null) {
            // TODO: define StreamOpened failed event
            HubEventVO.sendEvent(webSocket, "StreamOpened", null);
            log.warn("OpenStream failed for path: {}/sessionId: {}/contentId: {}/playIdx: {}", path, sessionId, contentId, playIdx);
            return;
        }

        Consumer<StreamSession.EventContext> sendEvent = (ctx) -> {
            HubEventVO.sendEvent(webSocket, ctx.name, ctx.payload);
            log.info("sendEvent: {} send => {}, {}, cost {} ms",
                    ctx.session, ctx.name, ctx.payload, System.currentTimeMillis() - ctx.start);
        };
        Consumer<StreamSession.DataContext> sendData = (ctx) -> {
            final int size = ctx.data.remaining();
            webSocket.send(ctx.data);
            log.info("sendData: {} send => {} bytes, cost {} ms",
                    ctx.session, size, System.currentTimeMillis() - ctx.start);
        };
        final int delayInMs = VarsUtil.extractValueAsInteger(path, "test_delay", 0);
        if (delayInMs > 0) {
            final Consumer<StreamSession.EventContext> performSendEvent = sendEvent;
            sendEvent = (ctx) -> {
                _scheduledExecutor.schedule(()->performSendEvent.accept(ctx), delayInMs, TimeUnit.MILLISECONDS);
            };
            final Consumer<StreamSession.DataContext> performSendData = sendData;
            sendData = (ctx) -> {
                _scheduledExecutor.schedule(()->performSendData.accept(ctx), delayInMs, TimeUnit.MILLISECONDS);
            };
        }
        final StreamSession _ss = new StreamSession(sendEvent, sendData, path, sessionId, contentId, playIdx);
        webSocket.setAttachment(_ss);

        _ss.onDataChange((ss) -> {
            ss.sendEvent(startInMs, "StreamOpened", null);
            return true;
        });
        bst.buildStream(_ss::appendData, (isOK) -> _ss.appendCompleted());
    }

    private BuildStreamTask getTaskOf(final String path) {
        try {
            if (path.contains("type=cp")) {
                return new CompositeStreamTask(path,
                        (pp) -> _scsService.asCache(new OSSStreamTask(pp, _ossClient)),
                        (bst) -> _scsService.asCache(bst),
                        this::selectTTSAgent,
                        this::selectCosyAgent);
            } else if (path.contains("type=tts")) {
                final BuildStreamTask bst = new TTSStreamTask(path, this::selectTTSAgent, null);
                return bst.key() != null ? _scsService.asCache(bst) : bst;
            } else if (path.contains("type=cosy")) {
                final BuildStreamTask bst = new CosyStreamTask(path, this::selectCosyAgent, null);
                return bst.key() != null ? _scsService.asCache(bst) : bst;
            } else {
                return _scsService.asCache(new OSSStreamTask(path, _ossClient));
            }
        } catch (Exception ex) {
            log.warn("getTaskOf failed: {}", ex.toString());
            return null;
        }
    }

    private void handleGetFileLenCommand(final HubCommandVO cmd, final WebSocket webSocket) {
        final long startInMs = System.currentTimeMillis();
        log.info("get file len:");
        final StreamSession ss = webSocket.getAttachment();
        if (ss == null) {
            log.warn("handleGetFileLenCommand: ss is null, just return 0");
            HubEventVO.sendEvent(webSocket, "GetFileLenResult", new PayloadGetFileLenResult(0));
            return;
        }
        ss.sendEvent(startInMs, "GetFileLenResult", new PayloadGetFileLenResult(ss.length()));
    }

    private void handleFileSeekCommand(final HubCommandVO cmd, final WebSocket webSocket) {
        final long startInMs = System.currentTimeMillis();
        final int offset = Integer.parseInt(cmd.getPayload().get("offset"));
        final int whence = Integer.parseInt(cmd.getPayload().get("whence"));
        log.info("file seek => offset: {}, whence: {}", offset, whence);
        final StreamSession ss = webSocket.getAttachment();
        if (ss == null) {
            log.warn("handleFileSeekCommand: ss is null, just return 0");
            HubEventVO.sendEvent(webSocket, "FileSeekResult", new PayloadFileSeekResult(0));
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

    private void handleFileReadCommand(final HubCommandVO cmd, final WebSocket webSocket) {
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

    private void handleFileTellCommand(final HubCommandVO cmd, final WebSocket webSocket) {
        final long startInMs = System.currentTimeMillis();
        final StreamSession ss = webSocket.getAttachment();
        if (ss == null) {
            log.warn("handleFileTellCommand: ss is null, just return 0");
            HubEventVO.sendEvent(webSocket, "FileTellResult", new PayloadFileSeekResult(0));
            return;
        }
        log.info("file tell: current pos: {}", ss.tell());
        ss.sendEvent(startInMs, "FileTellResult", new PayloadFileSeekResult(ss.tell()));
    }

    private void handleStopPlaybackCommand(final HubCommandVO cmd, final WebSocket webSocket) {
        final MediaSession session = webSocket.getAttachment();
        if (session == null) {
            log.error("StopPlayback: {} without Session, abort", webSocket.getRemoteSocketAddress());
            return;
        }
        session.stopCurrentAnyway();
    }

    private void handlePlayTTSCommand(final HubCommandVO cmd, final WebSocket webSocket) {
        final String text = cmd.getPayload().get("text");
        if (text == null ) {
            log.warn("PlayTTS: {} 's text is null, abort", webSocket.getRemoteSocketAddress());
            return;
        }
        final MediaSession session = webSocket.getAttachment();
        if (session == null) {
            log.error("PlayTTS: {} without Session, abort", webSocket.getRemoteSocketAddress());
            return;
        }

        final List<byte[]> bufs = new ArrayList<>();
        final long startInMs = System.currentTimeMillis();
        final TTSAgent agent = selectTTSAgent();
        final AtomicInteger idx = new AtomicInteger(0);
        final TTSTask task = new TTSTask(agent,
                (synthesizer)->synthesizer.setText(text),
                (bytes) -> {
                    final byte[] bytesArray = new byte[bytes.remaining()];
                    bytes.get(bytesArray, 0, bytesArray.length);
                    bufs.add(bytesArray);
                    log.info("{}: onData {} bytes", idx.incrementAndGet(), bytesArray.length);
                },
                (response)->{
                    log.info("handlePlayTTSCommand: gen pcm stream cost={} ms", System.currentTimeMillis() - startInMs);
                    session.stopCurrentAndStartPlay(new PlayPCMTask(0, 0,
                            _scheduledExecutor,
                            new ByteArrayListInputStream(bufs),
                            new SampleInfo(8000, 20, 16, 1),
                            webSocket,
                            session::stopCurrentIfMatch));
                },
                (response)-> log.warn("tts failed: {}", response));
        task.start();
    }

    private void handlePlaybackCommand(final HubCommandVO cmd, final WebSocket webSocket) {
        final MediaSession session = webSocket.getAttachment();
        if (session == null) {
            log.error("Playback: {} without Session, abort", webSocket.getRemoteSocketAddress());
            return;
        }

        final String file = cmd.getPayload().get("file");
        final String playbackId = cmd.getPayload().get("id");
        // {vars_playback_id=73cb4b57-0c54-42aa-98a9-9b81352c0cc4}/mnt/aicall/aispeech/dd_app_sb_3_0/c264515130674055869c16fcc2458109.wav
        if (file == null && playbackId == null) {
            log.warn("Playback: {} 's file & id is null, abort", webSocket.getRemoteSocketAddress());
            return;
        }
        log.info("handlePlaybackCommand: command payload ==> file:{}, id:{}", file, playbackId);
        if (file != null) {
            playbackByFile(file, cmd, session, webSocket);
        } else {
            playbackById(Integer.parseInt(playbackId), cmd, session, webSocket);
        }
    }

    private void playbackByFile(final String file, final HubCommandVO cmd, final MediaSession session, final WebSocket webSocket) {
        final String str_interval = cmd.getPayload().get("interval");
        final int prefixBegin = file.indexOf(_oss_match_prefix);
        if (-1 == prefixBegin) {
            log.warn("handlePlaybackCommand: can't match prefix for {}, ignore playback command!", file);
            return;
        }
        final String objectName = file.substring(prefixBegin + _oss_match_prefix.length());
        _ossDownloader.submit(() -> {
            try (final OSSObject ossObject = _ossClient.getObject(_oss_bucket, objectName)) {
                final ByteArrayOutputStream os = new ByteArrayOutputStream((int) ossObject.getObjectMetadata().getContentLength());
                ossObject.getObjectContent().transferTo(os);
                final int id = session.addPlaybackStream(os.toByteArray());
                final InputStream is = new ByteArrayInputStream(session.getPlaybackStream(id));
                final AudioInputStream audioInputStream = AudioSystem.getAudioInputStream(is);
                final AudioFormat format = audioInputStream.getFormat();

                // interval = 20 ms
                int interval = str_interval != null ? Integer.parseInt(str_interval) : 20;

                log.info("playbackByFile: sample rate: {}/interval: {}/channels: {}", format.getSampleRate(), interval, format.getChannels());
                session.stopCurrentAndStartPlay(new PlayPCMTask(id, 0,
                        _scheduledExecutor,
                        audioInputStream,
                        new SampleInfo((int) format.getSampleRate(), interval, format.getSampleSizeInBits(), format.getChannels()),
                        webSocket,
                        session::stopCurrentIfMatch));
            } catch (IOException | UnsupportedAudioFileException ex) {
                log.warn("playbackByFile: failed to load pcm: {}", ex.toString());
                throw new RuntimeException(ex);
            }
        });
    }

    private void playbackById(final int id, final HubCommandVO cmd, final MediaSession session, final WebSocket webSocket) {
        final int samples = getIntValueByName(cmd.getPayload(), "samples", 0);
        try {
            final byte[] bytes = session.getPlaybackStream(id);
            if (bytes == null) {
                log.warn("playbackById: failed to load bytes by id: {}", id);
                return;
            }
            final InputStream is = new ByteArrayInputStream(bytes);
            final AudioInputStream audioInputStream = AudioSystem.getAudioInputStream(is);
            final AudioFormat format = audioInputStream.getFormat();

            // interval = 20 ms
            int interval = 20;

            log.info("playbackById: sample rate: {}/interval: {}/channels: {}/samples: {}", format.getSampleRate(), interval, format.getChannels(), samples);
            final SampleInfo sampleInfo = new SampleInfo((int) format.getSampleRate(), interval, format.getSampleSizeInBits(), format.getChannels());
            if (samples > 0) {
                audioInputStream.skip((long) samples * sampleInfo.sampleSizeInBytes());
                log.info("playbackById: skip: {} samples", samples);
            }
            session.stopCurrentAndStartPlay(new PlayPCMTask(id, samples,
                    _scheduledExecutor,
                    audioInputStream,
                    sampleInfo,
                    webSocket,
                    session::stopCurrentIfMatch));
        } catch (IOException | UnsupportedAudioFileException ex) {
            log.warn("playbackById: failed to load pcm: {}", ex.toString());
            throw new RuntimeException(ex);
        }
    }

    private int getIntValueByName(final Map<String, String> map, final String name, final int defaultValue) {
        final String value = map.get(name);
        try {
            return value == null ? defaultValue : Integer.parseInt(value);
        } catch (NumberFormatException ex) {
            return defaultValue;
        }
    }

    private void handleStartTranscriptionCommand(final HubCommandVO cmd, final WebSocket webSocket) {
        final MediaSession session = webSocket.getAttachment();
        if (session == null) {
            log.error("StartTranscription: {} without Session, abort", webSocket.getRemoteSocketAddress());
            return;
        }

        SpeechTranscriber speechTranscriber = null;

        try {
            session.lock();

            if (!session.startTranscription()) {
                log.warn("StartTranscription: {}'s Session startTranscription already, ignore", webSocket.getRemoteSocketAddress());
                return;
            }

            final long startConnectingInMs = System.currentTimeMillis();
            final ASRAgent agent = selectASRAgent();
            session.setAsrAgent(agent);

            speechTranscriber = buildSpeechTranscriber(agent, buildTranscriberListener(session, webSocket, agent, session.get_sessionId(), startConnectingInMs));
            session.setSpeechTranscriber(speechTranscriber);
        } catch (Exception ex) {
            // TODO: close websocket?
            log.error("StartTranscription: failed: {}", ex.toString());
            throw new RuntimeException(ex);
        } finally {
            session.unlock();
        }

        try {
            speechTranscriber.start();
        } catch (Exception ex) {
            log.error("speechTranscriber.start() error: {}", ex.toString());
        }
    }

    private SpeechTranscriber buildSpeechTranscriber(final ASRAgent agent, final SpeechTranscriberListener listener) throws Exception {
        //创建实例、建立连接。
        final SpeechTranscriber transcriber = agent.buildSpeechTranscriber(listener);

        //输入音频编码方式。
        transcriber.setFormat(InputFormatEnum.PCM);
        //输入音频采样率。
        //transcriber.setSampleRate(SampleRateEnum.SAMPLE_RATE_16K);
        transcriber.setSampleRate(SampleRateEnum.SAMPLE_RATE_8K);
        //是否返回中间识别结果。
        transcriber.setEnableIntermediateResult(true);
        //是否生成并返回标点符号。
        transcriber.setEnablePunctuation(true);
        //是否将返回结果规整化，比如将一百返回为100。
        transcriber.setEnableITN(true);

        //设置vad断句参数。默认值：800ms，有效值：200ms～2000ms。
        //transcriber.addCustomedParam("max_sentence_silence", 600);
        //设置是否语义断句。
        //transcriber.addCustomedParam("enable_semantic_sentence_detection",false);
        //设置是否开启过滤语气词，即声音顺滑。
        //transcriber.addCustomedParam("disfluency",true);
        //设置是否开启词模式。
        //transcriber.addCustomedParam("enable_words",true);
        //设置vad噪音阈值参数，参数取值为-1～+1，如-0.9、-0.8、0.2、0.9。
        //取值越趋于-1，判定为语音的概率越大，亦即有可能更多噪声被当成语音被误识别。
        //取值越趋于+1，判定为噪音的越多，亦即有可能更多语音段被当成噪音被拒绝识别。
        //该参数属高级参数，调整需慎重和重点测试。
        //transcriber.addCustomedParam("speech_noise_threshold",0.3);
        //设置训练后的定制语言模型id。
        //transcriber.addCustomedParam("customization_id","你的定制语言模型id");
        //设置训练后的定制热词id。
        //transcriber.addCustomedParam("vocabulary_id","你的定制热词id");
        return transcriber;
    }

    @NotNull
    private SpeechTranscriberListener buildTranscriberListener(final MediaSession session,
                                                               final WebSocket webSocket,
                                                               final ASRAgent account,
                                                               final String sessionId,
                                                               final long startConnectingInMs) {
        return new SpeechTranscriberListener() {
            @Override
            public void onTranscriberStart(final SpeechTranscriberResponse response) {
                //task_id是调用方和服务端通信的唯一标识，遇到问题时，需要提供此task_id。
                log.info("onTranscriberStart: sessionId={}, task_id={}, name={}, status={}, cost={} ms",
                        sessionId,
                        response.getTaskId(),
                        response.getName(),
                        response.getStatus(),
                        System.currentTimeMillis() - startConnectingInMs);
                notifyTranscriptionStarted(webSocket, account, response);
            }

            @Override
            public void onSentenceBegin(final SpeechTranscriberResponse response) {
                log.info("onSentenceBegin: sessionId={}, task_id={}, name={}, status={}",
                        sessionId, response.getTaskId(), response.getName(), response.getStatus());
                notifySentenceBegin(webSocket, response);
            }

            @Override
            public void onSentenceEnd(final SpeechTranscriberResponse response) {
                log.info("onSentenceEnd: sessionId={}, task_id={}, name={}, status={}, index={}, result={}, confidence={}, begin_time={}, time={}",
                        sessionId,
                        response.getTaskId(),
                        response.getName(),
                        response.getStatus(),
                        response.getTransSentenceIndex(),
                        response.getTransSentenceText(),
                        response.getConfidence(),
                        response.getSentenceBeginTime(),
                        response.getTransSentenceTime());
                notifySentenceEnd(webSocket, response);
            }

            @Override
            public void onTranscriptionResultChange(final SpeechTranscriberResponse response) {
                log.info("onTranscriptionResultChange: sessionId={}, task_id={}, name={}, status={}, index={}, result={}, time={}",
                        sessionId,
                        response.getTaskId(),
                        response.getName(),
                        response.getStatus(),
                        response.getTransSentenceIndex(),
                        response.getTransSentenceText(),
                        response.getTransSentenceTime());
                notifyTranscriptionResultChanged(webSocket, response);
            }

            @Override
            public void onTranscriptionComplete(final SpeechTranscriberResponse response) {
                log.info("onTranscriptionComplete: sessionId={}, task_id={}, name={}, status={}",
                        sessionId, response.getTaskId(), response.getName(), response.getStatus());
                notifyTranscriptionCompleted(webSocket, account, response);
            }

            @Override
            public void onFail(final SpeechTranscriberResponse response) {
                log.warn("onFail: sessionId={}, task_id={}, status={}, status_text={}",
                        sessionId,
                        response.getTaskId(),
                        response.getStatus(),
                        response.getStatusText());
                session.notifySpeechTranscriberFail(response);
            }
        };
    }

    private void notifyTranscriptionStarted(final WebSocket webSocket, final ASRAgent account, final SpeechTranscriberResponse response) {
        final MediaSession session = webSocket.getAttachment();
        session.transcriptionStarted();
        account.incConnected();
        try {
            HubEventVO.sendEvent(webSocket, "TranscriptionStarted", (Void) null);
        } catch (WebsocketNotConnectedException ex) {
            log.info("ws disconnected when sendEvent TranscriptionStarted: {}", ex.toString());
        }
    }

    private void notifySentenceBegin(final WebSocket webSocket, final SpeechTranscriberResponse response) {
        try {
            HubEventVO.sendEvent(webSocket, "SentenceBegin",
                    new PayloadSentenceBegin(response.getTransSentenceIndex(), response.getTransSentenceTime()));
        } catch (WebsocketNotConnectedException ex) {
            log.info("ws disconnected when sendEvent SentenceBegin: {}", ex.toString());
        }
    }

    private void notifySentenceEnd(final WebSocket webSocket, final SpeechTranscriberResponse response) {
        try {
            HubEventVO.sendEvent(webSocket, "SentenceEnd",
                    new PayloadSentenceEnd(response.getTransSentenceIndex(),
                            response.getTransSentenceTime(),
                            response.getSentenceBeginTime(),
                            response.getTransSentenceText(),
                            response.getConfidence()));
        } catch (WebsocketNotConnectedException ex) {
            log.info("ws disconnected when sendEvent SentenceEnd: {}", ex.toString());
        }
    }

    private void notifyTranscriptionResultChanged(final WebSocket webSocket, final SpeechTranscriberResponse response) {
        try {
            HubEventVO.sendEvent(webSocket, "TranscriptionResultChanged",
                    new PayloadTranscriptionResultChanged(response.getTransSentenceIndex(),
                            response.getTransSentenceTime(),
                            response.getTransSentenceText()));
        } catch (WebsocketNotConnectedException ex) {
            log.info("ws disconnected when sendEvent TranscriptionResultChanged: {}", ex.toString());
        }
    }

    private void notifyTranscriptionCompleted(final WebSocket webSocket, final ASRAgent account, final SpeechTranscriberResponse response) {
        try {
            // TODO: account.dec??
            HubEventVO.sendEvent(webSocket, "TranscriptionCompleted", (Void)null);
        } catch (WebsocketNotConnectedException ex) {
            log.info("ws disconnected when sendEvent TranscriptionCompleted: {}", ex.toString());
        }
    }

    private void handleStopTranscriptionCommand(final HubCommandVO cmd, final WebSocket webSocket) {
        stopAndCloseTranscriber(webSocket);
        webSocket.close();
    }

    private static void stopAndCloseTranscriber(final WebSocket webSocket) {
        final MediaSession session = webSocket.getAttachment();
        if (session == null) {
            log.error("stopAndCloseTranscriber: {} without Session, abort", webSocket.getRemoteSocketAddress());
            return;
        }

        session.stopAndCloseTranscriber(webSocket);
    }

    @PreDestroy
    public void stop() throws InterruptedException {

        _wsServer.stop();
        _nlsClient.shutdown();

        _nlsAuthExecutor.shutdownNow();
        _sessionExecutor.shutdownNow();
        _scheduledExecutor.shutdownNow();
        _ossDownloader.shutdownNow();

        log.info("ASR-Hub: shutdown");
    }
}