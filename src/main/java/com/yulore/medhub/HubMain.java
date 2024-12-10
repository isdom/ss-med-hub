package com.yulore.medhub;

import com.alibaba.fastjson.JSON;
import com.alibaba.nls.client.protocol.InputFormatEnum;
import com.alibaba.nls.client.protocol.NlsClient;
import com.alibaba.nls.client.protocol.OutputFormatEnum;
import com.alibaba.nls.client.protocol.SampleRateEnum;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriber;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriberListener;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriberResponse;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.OSSObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mgnt.utils.StringUnicodeEncoderDecoder;
import com.tencent.asrv2.AsrConstant;
import com.tencent.asrv2.SpeechRecognizer;
import com.tencent.asrv2.SpeechRecognizerListener;
import com.tencent.asrv2.SpeechRecognizerResponse;
import com.tencent.core.ws.SpeechClient;
import com.yulore.medhub.api.ScriptApi;
import com.yulore.medhub.nls.*;
import com.yulore.medhub.session.*;
import com.yulore.medhub.stream.*;
import com.yulore.medhub.stream.StreamCacheService;
import com.yulore.medhub.task.PlayPCMTask;
import com.yulore.medhub.task.PlayStreamPCMTask;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
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

    @Value("#{${nls.txasr}}")
    private Map<String,String> _all_txasr;

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

    @Value("${session.match_media}")
    private String _match_media;

    @Value("${session.match_call}")
    private String _match_call;

    @Value("${session.match_playback}")
    private String _match_playback;

    final List<ASRAgent> _asrAgents = new ArrayList<>();
    final List<TTSAgent> _ttsAgents = new ArrayList<>();
    final List<CosyAgent> _cosyAgents = new ArrayList<>();
    final List<TxASRAgent> _txasrAgents = new ArrayList<>();

    private ScheduledExecutorService _nlsAuthExecutor;
    
    WebSocketServer _wsServer;

    private NlsClient _nlsClient;

    private SpeechClient _txClient;

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

    @Value("${oss.path}")
    private String _oss_path;

    @Value("${oss.match_prefix}")
    private String _oss_match_prefix;

    private OSS _ossClient;

    private ExecutorService _ossAccessExecutor;

    private final AtomicInteger _currentWSConnection = new AtomicInteger(0);

    @Autowired
    private StreamCacheService _scsService;

    @Resource
    private ScriptApi _scriptApi;

    @PostConstruct
    public void start() {
        //创建NlsClient实例应用全局创建一个即可。生命周期可和整个应用保持一致，默认服务地址为阿里云线上服务地址。
        _nlsClient = new NlsClient(_nls_url, "invalid_token");
        _ossClient = new OSSClientBuilder().build(_oss_endpoint, _oss_access_key_id, _oss_access_key_secret);

        _txClient = new SpeechClient(AsrConstant.DEFAULT_RT_REQ_URL);

        initNlsAgents(_nlsClient);

        _ossAccessExecutor = Executors.newFixedThreadPool(NettyRuntime.availableProcessors() * 2, new DefaultThreadFactory("ossAccessExecutor"));
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
                        if (clientHandshake.getResourceDescriptor() != null && clientHandshake.getResourceDescriptor().startsWith(_match_media)) {
                            // init MediaSession attach with webSocket
                            final String sessionId = clientHandshake.getFieldValue("x-sessionid");
                            final MediaSession session = new MediaSession(sessionId, _test_enable_delay, _test_delay_ms,
                                    _test_enable_disconnect, _test_disconnect_probability, ()->webSocket.close(1006, "test_disconnect"));
                            webSocket.setAttachment(session);
                            session.scheduleCheckIdle(_scheduledExecutor, _check_idle_interval_ms,
                                    ()->HubEventVO.<Void>sendEvent(webSocket, "CheckIdle", null));
                            log.info("ws path match: {}, using ws as MediaSession {}", _match_media, sessionId);
                        } else if (clientHandshake.getResourceDescriptor() != null && clientHandshake.getResourceDescriptor().startsWith(_match_call)) {
                            // init CallSession attach with webSocket
                            final CallSession session = new CallSession(_scriptApi, ()->webSocket.close(1000, "hangup"), _oss_bucket, _oss_path);
                            webSocket.setAttachment(session);
                            session.scheduleCheckIdle(_scheduledExecutor, _check_idle_interval_ms, session::checkIdle);

                            log.info("ws path match: {}, using ws as CallSession", _match_call);
                        } else if (clientHandshake.getResourceDescriptor() != null && clientHandshake.getResourceDescriptor().startsWith(_match_playback)) {
                            // init PlaybackSession attach with webSocket
                            final String path = clientHandshake.getResourceDescriptor();
                            final int varsBegin = path.indexOf('?');
                            final String sessionId = varsBegin > 0 ? VarsUtil.extractValue(path.substring(varsBegin + 1), "sessionId") : "unknown";
                            final PlaybackSession playbackSession = new PlaybackSession(sessionId);
                            webSocket.setAttachment(playbackSession);
                            log.info("ws path match: {}, using ws as PlaybackSession: [{}]", _match_playback, playbackSession.sessionId());
                            final CallSession callSession = CallSession.findBy(sessionId);
                            if (callSession == null) {
                                log.info("can't find callSession by sessionId: {}, ignore", sessionId);
                                return;
                            }
                            callSession.attach(playbackSession, (_path) -> playbackOn(_path, callSession, playbackSession, webSocket));
                        } else {
                            log.info("ws path {} !NOT! match: {}, NOT MediaSession: {}",
                                    clientHandshake.getResourceDescriptor(), _match_media, webSocket.getRemoteSocketAddress());
                        }
                    }

                    @Override
                    public void onClose(final WebSocket webSocket, final int code, final String reason, final boolean remote) {
                        final Object attachment = webSocket.getAttachment();
                        if (attachment instanceof MediaSession session) {
                            stopAndCloseTranscriber(webSocket);
                            session.close();
                            log.info("wscount/{}: closed {} with exit code {} additional info: {}, MediaSession-id {}",
                                    _currentWSConnection.decrementAndGet(),
                                    webSocket.getRemoteSocketAddress(), code, reason, session.sessionId());
                        } else if (attachment instanceof CallSession session) {
                            stopAndCloseTranscriber(webSocket);
                            session.close();
                            log.info("wscount/{}: closed {} with exit code {} additional info: {}, CallSession-id: {}",
                                    _currentWSConnection.decrementAndGet(),
                                    webSocket.getRemoteSocketAddress(), code, reason, session.sessionId());
                        } else if (attachment instanceof StreamSession session) {
                            session.close();
                            log.info("wscount/{}: closed {} with exit code {} additional info: {}, StreamSession-id: {}",
                                    _currentWSConnection.decrementAndGet(),
                                    webSocket.getRemoteSocketAddress(), code, reason, session.sessionId());
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
                        final Object attachment = webSocket.getAttachment();
                        if (attachment instanceof ASRSession session) {
                            handleASRData(bytes, session);
                            return;
                        } else if (attachment instanceof StreamSession session) {
                            _sessionExecutor.submit(()-> handleFileWriteCommand(bytes, session, webSocket));
                            return;
                        }
                        log.error("onMessage(Binary): {} without any Session, ignore", webSocket.getRemoteSocketAddress());

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

    private void playbackOn(final String path, final CallSession callSession, final PlaybackSession playbackSession, final WebSocket webSocket) {
        // interval = 20 ms
        int interval = 20;
        log.info("playbackOn: {} => sample rate: {}/interval: {}/channels: {}", path, 16000, interval, 1);
        final PlayStreamPCMTask task = new PlayStreamPCMTask(
                path,
                _scheduledExecutor,
                new SampleInfo(16000, interval, 16, 1),
                webSocket,
                // session::stopCurrentIfMatch
                (_task) -> {
                    log.info("PlayStreamPCMTask {} stopped with completed: {}", _task.path(), _task.isCompleted());
                    callSession.notifyPlaybackStop(_task);
                    playbackSession.notifyPlaybackStop(_task);
                }
        );
        callSession.notifyPlaybackStart(task);
        playbackSession.notifyPlaybackStart(task);
        final BuildStreamTask bst = getTaskOf(path, true, 16000);
        if (bst != null) {
            playbackSession.attach(task);
            bst.buildStream(task::appendData, (ignore)->task.appendCompleted());
        }

        //session.stopCurrentAndStartPlay();
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

        _txasrAgents.clear();
        if (_all_txasr != null) {
            for (Map.Entry<String, String> entry : _all_txasr.entrySet()) {
                log.info("txasr: {} / {}", entry.getKey(), entry.getValue());
                final String[] values = entry.getValue().split(" ");
                log.info("txasr values detail: {}", Arrays.toString(values));
                final TxASRAgent agent = TxASRAgent.parse(entry.getKey(), entry.getValue());
                if (null == agent) {
                    log.warn("txasr init failed by: {}/{}", entry.getKey(), entry.getValue());
                } else {
                    agent.setClient(_txClient);
                    _txasrAgents.add(agent);
                }
            }
        }
        log.info("txasr agent init, count:{}", _txasrAgents.size());

        _nlsAuthExecutor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("nlsAuthExecutor"));
        _nlsAuthExecutor.scheduleAtFixedRate(this::checkAndUpdateNlsToken, 0, 10, TimeUnit.SECONDS);
    }

    private ASRAgent selectASRAgent() {
        for (ASRAgent agent : _asrAgents) {
            final ASRAgent selected = agent.checkAndSelectIfHasIdle();
            if (null != selected) {
                log.info("select asr({}): {}/{}", agent.getName(), agent.get_connectingOrConnectedCount().get(), agent.getLimit());
                return selected;
            }
        }
        throw new RuntimeException("all asr agent has full");
    }

    private TTSAgent selectTTSAgent() {
        for (TTSAgent agent : _ttsAgents) {
            final TTSAgent selected = agent.checkAndSelectIfHasIdle();
            if (null != selected) {
                log.info("select tts({}): {}/{}", agent.getName(), agent.get_connectingOrConnectedCount().get(), agent.getLimit());
                return selected;
            }
        }
        throw new RuntimeException("all tts agent has full");
    }

    private CosyAgent selectCosyAgent() {
        for (CosyAgent agent : _cosyAgents) {
            final CosyAgent selected = agent.checkAndSelectIfHasIdle();
            if (null != selected) {
                log.info("select cosy({}): {}/{}", agent.getName(), agent.get_connectingOrConnectedCount().get(), agent.getLimit());
                return selected;
            }
        }
        throw new RuntimeException("all cosy agent has full");
    }

    private TxASRAgent selectTxASRAgent() {
        for (TxASRAgent agent : _txasrAgents) {
            final TxASRAgent selected = agent.checkAndSelectIfHasIdle();
            if (null != selected) {
                log.info("select txasr({}): {}/{}", agent.getName(), agent.get_connectingOrConnectedCount().get(), agent.getLimit());
                return selected;
            }
        }
        throw new RuntimeException("all txasr agent has full");
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

    private void handleASRData(final ByteBuffer bytes, final ASRSession session) {
        if (session.transmit(bytes)) {
            // transmit success
            if ((session.transmitCount() % 50) == 0) {
                log.debug("{}: transmit 50 times.", session.sessionId());
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
        } else if ("PausePlayback".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handlePausePlaybackCommand(cmd, webSocket));
        } else if ("ResumePlayback".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleResumePlaybackCommand(cmd, webSocket));
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
        } else if ("UserAnswer".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleUserAnswerCommand(cmd, webSocket));
        } else {
            log.warn("handleHubCommand: Unknown Command: {}", cmd);
        }
    }

    private void handleUserAnswerCommand(final HubCommandVO cmd, final WebSocket webSocket) {
        final CallSession session = webSocket.getAttachment();
        if (session == null) {
            log.error("UserAnswer: {} without CallSession, abort", webSocket.getRemoteSocketAddress());
            return;
        }
        session.notifyUserAnswer(webSocket);
    }

    Consumer<StreamSession.EventContext> buildSendEvent(final WebSocket webSocket, final int delayInMs) {
        final Consumer<StreamSession.EventContext> performSendEvent = (ctx) -> {
            HubEventVO.sendEvent(webSocket, ctx.name, ctx.payload);
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

    private void handleOpenStreamCommand(final HubCommandVO cmd, final WebSocket webSocket) {
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
                HubEventVO.sendEvent(webSocket, "StreamOpened", null);
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
                final BuildStreamTask bst = new TTSStreamTask(path, this::selectTTSAgent, (synthesizer) -> {
                    synthesizer.setFormat(removeWavHdr ? OutputFormatEnum.PCM : OutputFormatEnum.WAV);
                    synthesizer.setSampleRate(sampleRate);
                });
                return bst.key() != null ? _scsService.asCache(bst) : bst;
            } else if (path.contains("type=cosy")) {
                final BuildStreamTask bst = new CosyStreamTask(path, this::selectCosyAgent, (synthesizer) -> {
                    synthesizer.setFormat(removeWavHdr ? OutputFormatEnum.PCM : OutputFormatEnum.WAV);
                    synthesizer.setSampleRate(sampleRate);
                });
                return bst.key() != null ? _scsService.asCache(bst) : bst;
            } else {
                return _scsService.asCache(new OSSStreamTask(path, _ossClient, removeWavHdr));
            }
        } catch (Exception ex) {
            log.warn("getTaskOf failed: {}", ex.toString());
            return null;
        }
    }

    private BuildStreamTask cvo2bst(final CompositeVO cvo) {
        if (cvo.getBucket() != null) {
            log.info("support CVO => OSS Stream: {}", cvo);
            return new OSSStreamTask("{bucket=" + cvo.getBucket() + "}" + cvo.getObject(), _ossClient, true);
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
        return new CosyStreamTask(cvo2cosy(cvo), this::selectCosyAgent, (synthesizer) -> {
            synthesizer.setVolume(50);
            //设置返回音频的编码格式
            synthesizer.setFormat(OutputFormatEnum.PCM);
            //设置返回音频的采样率。
            synthesizer.setSampleRate(SampleRateEnum.SAMPLE_RATE_16K);
        });
    }

    private BuildStreamTask genTtsStreamTask(final CompositeVO cvo) {
        return new TTSStreamTask(cvo2tts(cvo), this::selectTTSAgent, (synthesizer) -> {
            //设置返回音频的编码格式
            synthesizer.setFormat(OutputFormatEnum.PCM);
            //设置返回音频的采样率
            synthesizer.setSampleRate(SampleRateEnum.SAMPLE_RATE_16K);
        });
    }

    static private String cvo2tts(final CompositeVO cvo) {
        // {type=tts,voice=xxx,url=ws://172.18.86.131:6789/playback,vars_playback_id=<uuid>,content_id=2088788,vars_start_timestamp=1732028219711854,text='StringUnicodeEncoderDecoder.encodeStringToUnicodeSequence(content)'}
        //          unused.wav
        return String.format("{type=tts,cache=%s,voice=%s,text=%s}tts.wav", cvo.getCache(), cvo.getVoice(),
                StringUnicodeEncoderDecoder.encodeStringToUnicodeSequence(cvo.getText()));
    }

    static private String cvo2cosy(final CompositeVO cvo) {
        // eg: {type=cosy,voice=xxx,url=ws://172.18.86.131:6789/cosy,vars_playback_id=<uuid>,content_id=2088788,vars_start_timestamp=1732028219711854,text='StringUnicodeEncoderDecoder.encodeStringToUnicodeSequence(content)'}
        //          unused.wav
        return String.format("{type=cosy,cache=%s,voice=%s,text=%s}cosy.wav", cvo.getCache(), cvo.getVoice(),
                StringUnicodeEncoderDecoder.encodeStringToUnicodeSequence(cvo.getText()));
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

    private void handleFileWriteCommand(final ByteBuffer bytes, final StreamSession ss, final WebSocket webSocket) {
        final long startInMs = System.currentTimeMillis();
        final int written = ss.writeToStream(bytes);
        ss.sendEvent(startInMs, "FileWriteResult", new PayloadFileWriteResult(written));
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
        log.info("[{}]: handlePlaybackCommand: command payload ==> file:{}, id:{}", session.sessionId(), file, playbackId);
        if (file != null) {
            playbackByFile(file, cmd, session, webSocket);
        } else {
            playbackById(Integer.parseInt(playbackId), cmd, session, webSocket);
        }
    }

    private void handleStopPlaybackCommand(final HubCommandVO cmd, final WebSocket webSocket) {
        final MediaSession session = webSocket.getAttachment();
        if (session == null) {
            log.error("StopPlayback: {} without Session, abort", webSocket.getRemoteSocketAddress());
            return;
        }
        log.info("[{}]: handleStopPlaybackCommand", session.sessionId());
        session.stopCurrentAnyway();
    }

    private void handlePausePlaybackCommand(final HubCommandVO cmd, final WebSocket webSocket) {
        final MediaSession session = webSocket.getAttachment();
        if (session == null) {
            log.error("PausePlayback: {} without Session, abort", webSocket.getRemoteSocketAddress());
            return;
        }
        log.info("[{}]: handlePausePlaybackCommand", session.sessionId());
        session.pauseCurrentAnyway();
    }

    private void handleResumePlaybackCommand(final HubCommandVO cmd, final WebSocket webSocket) {
        final MediaSession session = webSocket.getAttachment();
        if (session == null) {
            log.error("ResumePlayback: {} without Session, abort", webSocket.getRemoteSocketAddress());
            return;
        }
        log.info("[{}]: handleResumePlaybackCommand", session.sessionId());
        session.resumeCurrentAnyway();
    }

    private void playbackByFile(final String file, final HubCommandVO cmd, final MediaSession session, final WebSocket webSocket) {
        final String str_interval = cmd.getPayload().get("interval");
        final int prefixBegin = file.indexOf(_oss_match_prefix);
        if (-1 == prefixBegin) {
            log.warn("handlePlaybackCommand: can't match prefix for {}, ignore playback command!", file);
            return;
        }
        final String objectName = file.substring(prefixBegin + _oss_match_prefix.length());
        _ossAccessExecutor.submit(() -> {
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
        final String str_interval = cmd.getPayload().get("interval");
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
            int interval = str_interval != null ? Integer.parseInt(str_interval) : 20;

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
        final String provider = cmd.getPayload() != null ? cmd.getPayload().get("provider") : null;
        final ASRSession session = webSocket.getAttachment();
        if (session == null) {
            log.error("StartTranscription: {} without ASRSession, abort", webSocket.getRemoteSocketAddress());
            return;
        }

        try {
            session.lock();

            if (!session.startTranscription()) {
                log.warn("StartTranscription: {}'s Session startTranscription already, ignore", webSocket.getRemoteSocketAddress());
                return;
            }

            if ("tx".equals(provider)) {
                startWithTxasr(webSocket, session);
            } else {
                startWithAliasr(webSocket, session);
            }
        } catch (Exception ex) {
            // TODO: close websocket?
            log.error("StartTranscription: failed: {}", ex.toString());
            throw new RuntimeException(ex);
        } finally {
            session.unlock();
        }
    }

    private void startWithTxasr(final WebSocket webSocket, final ASRSession session) throws Exception {
        final long startConnectingInMs = System.currentTimeMillis();
        final TxASRAgent agent = selectTxASRAgent();

        final SpeechRecognizer speechRecognizer = buildSpeechRecognizer(agent, buildRecognizerListener(session, webSocket, agent, session.sessionId(), startConnectingInMs));

        session.setASR(()-> {
            try {
                agent.decConnection();
                try {
                    //通知服务端语音数据发送完毕，等待服务端处理完成。
                    long now = System.currentTimeMillis();
                    log.info("recognizer wait for complete");
                    speechRecognizer.stop();
                    log.info("recognizer stop() latency : {} ms", System.currentTimeMillis() - now);
                } catch (final Exception ex) {
                    log.warn("handleStopAsrCommand error: {}", ex.toString());
                }

                speechRecognizer.close();

                if (session.isTranscriptionStarted()) {
                    // 对于已经标记了 TranscriptionStarted 的会话, 将其使用的 ASR Account 已连接通道减少一
                    agent.decConnected();
                }
            } finally {
                if (agent != null) {
                    log.info("release txasr({}): {}/{}", agent.getName(), agent.get_connectingOrConnectedCount().get(), agent.getLimit());
                }
            }
        }, (bytes) -> speechRecognizer.write(bytes.array()));

        try {
            speechRecognizer.start();
        } catch (Exception ex) {
            log.error("recognizer.start() error: {}", ex.toString());
        }
    }

    private SpeechRecognizer buildSpeechRecognizer(final TxASRAgent agent, final SpeechRecognizerListener listener) throws Exception {
        //创建实例、建立连接。
        final SpeechRecognizer recognizer = agent.buildSpeechRecognizer(listener);

        /*
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
         */

        return recognizer;
    }

    private SpeechRecognizerListener buildRecognizerListener(final ASRSession session,
                                                             final WebSocket webSocket,
                                                             final TxASRAgent account,
                                                             final String sessionId,
                                                             final long startConnectingInMs) {
        // https://cloud.tencent.com/document/product/1093/48982
        return new SpeechRecognizerListener() {
            @Override
            public void onRecognitionStart(final SpeechRecognizerResponse response) {
                log.info("{} sessionId={},voice_id:{},{},cost={} ms", "onRecognitionStart",
                        sessionId,
                        response.getVoiceId(),
                        JSON.toJSONString(response),
                        System.currentTimeMillis() - startConnectingInMs);
                // notifyTranscriptionStarted(webSocket, account, response);
                session.transcriptionStarted();
                account.incConnected();
                try {
                    HubEventVO.sendEvent(webSocket, "TranscriptionStarted", (Void) null);
                } catch (WebsocketNotConnectedException ex) {
                    log.info("ws disconnected when sendEvent TranscriptionStarted: {}", ex.toString());
                }
            }

            @Override
            public void onSentenceBegin(final SpeechRecognizerResponse response) {
                log.info("{} sessionId={},voice_id:{},{}", "onSentenceBegin",
                        sessionId,
                        response.getVoiceId(),
                        JSON.toJSONString(response));
                notifySentenceBegin(webSocket,
                        new PayloadSentenceBegin(response.getResult().getIndex(), response.getResult().getStartTime().intValue()));
            }

            @Override
            public void onSentenceEnd(final SpeechRecognizerResponse response) {
                log.info("{} sessionId={},voice_id:{},{}", "onSentenceEnd",
                        sessionId,
                        response.getVoiceId(),
                        JSON.toJSONString(response));
                notifySentenceEnd(webSocket,
                        new PayloadSentenceEnd(response.getResult().getIndex(),
                                response.getResult().getEndTime().intValue(),
                                response.getResult().getStartTime().intValue(),
                                response.getResult().getVoiceTextStr(),
                                0));
            }

            @Override
            public void onRecognitionResultChange(final SpeechRecognizerResponse response) {
                log.info("{} sessionId={},voice_id:{},{}", "onRecognitionResultChange",
                        sessionId,
                        response.getVoiceId(),
                        JSON.toJSONString(response));
                notifyTranscriptionResultChanged(webSocket,
                        new PayloadTranscriptionResultChanged(response.getResult().getIndex(),
                                response.getResult().getEndTime().intValue(),
                                response.getResult().getVoiceTextStr()));
            }

            @Override
            public void onRecognitionComplete(final SpeechRecognizerResponse response) {
                log.info("{} sessionId={},voice_id:{},{}", "onRecognitionComplete",
                        sessionId,
                        response.getVoiceId(),
                        JSON.toJSONString(response));
                notifyTranscriptionCompleted(webSocket);
            }

            @Override
            public void onFail(final SpeechRecognizerResponse response) {
                log.warn("{} sessionId={},voice_id:{},{}", "onFail",
                        sessionId,
                        response.getVoiceId(),
                        JSON.toJSONString(response));
                session.notifySpeechTranscriberFail();
            }

            @Override
            public void onMessage(final SpeechRecognizerResponse response) {
                log.info("{} voice_id:{},{}", "onMessage", response.getVoiceId(), JSON.toJSONString(response));
            }
        };
    }

    private void startWithAliasr(final WebSocket webSocket, final ASRSession session) throws Exception {
        final long startConnectingInMs = System.currentTimeMillis();
        final ASRAgent agent = selectASRAgent();

        final SpeechTranscriber speechTranscriber = buildSpeechTranscriber(agent, buildTranscriberListener(session, webSocket, agent, session.sessionId(), startConnectingInMs));

        session.setASR(()-> {
            try {
                agent.decConnection();
                try {
                    //通知服务端语音数据发送完毕，等待服务端处理完成。
                    long now = System.currentTimeMillis();
                    log.info("transcriber wait for complete");
                    speechTranscriber.stop();
                    log.info("transcriber stop() latency : {} ms", System.currentTimeMillis() - now);
                } catch (final Exception ex) {
                    log.warn("handleStopAsrCommand error: {}", ex.toString());
                }

                speechTranscriber.close();

                if (session.isTranscriptionStarted()) {
                    // 对于已经标记了 TranscriptionStarted 的会话, 将其使用的 ASR Account 已连接通道减少一
                    agent.decConnected();
                }
            } finally {
                if (agent != null) {
                    log.info("release asr({}): {}/{}", agent.getName(), agent.get_connectingOrConnectedCount().get(), agent.getLimit());
                }
            }
        }, (bytes) -> speechTranscriber.send(bytes.array()));

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

    private SpeechTranscriberListener buildTranscriberListener(final ASRSession session,
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
                notifySentenceBegin(webSocket,
                        new PayloadSentenceBegin(response.getTransSentenceIndex(), response.getTransSentenceTime()));
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
                notifySentenceEnd(webSocket,
                        new PayloadSentenceEnd(response.getTransSentenceIndex(),
                            response.getTransSentenceTime(),
                            response.getSentenceBeginTime(),
                            response.getTransSentenceText(),
                            response.getConfidence()));
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
                notifyTranscriptionResultChanged(webSocket,
                        new PayloadTranscriptionResultChanged(response.getTransSentenceIndex(),
                            response.getTransSentenceTime(),
                            response.getTransSentenceText()));
            }

            @Override
            public void onTranscriptionComplete(final SpeechTranscriberResponse response) {
                log.info("onTranscriptionComplete: sessionId={}, task_id={}, name={}, status={}",
                        sessionId,
                        response.getTaskId(),
                        response.getName(),
                        response.getStatus());
                notifyTranscriptionCompleted(webSocket);
            }

            @Override
            public void onFail(final SpeechTranscriberResponse response) {
                log.warn("onFail: sessionId={}, task_id={}, status={}, status_text={}",
                        sessionId,
                        response.getTaskId(),
                        response.getStatus(),
                        response.getStatusText());
                session.notifySpeechTranscriberFail();
            }
        };
    }

    private void notifyTranscriptionStarted(final WebSocket webSocket, final ASRAgent account, final SpeechTranscriberResponse response) {
        final ASRSession session = webSocket.getAttachment();
        session.transcriptionStarted();
        account.incConnected();
        try {
            HubEventVO.sendEvent(webSocket, "TranscriptionStarted", (Void) null);
        } catch (WebsocketNotConnectedException ex) {
            log.info("ws disconnected when sendEvent TranscriptionStarted: {}", ex.toString());
        }
    }

    private void notifySentenceBegin(final WebSocket webSocket, final PayloadSentenceBegin payload) {
        try {
            if (webSocket.getAttachment() instanceof ASRSession session) {
                session.notifySentenceBegin(payload);
            }
            HubEventVO.sendEvent(webSocket, "SentenceBegin", payload);
        } catch (WebsocketNotConnectedException ex) {
            log.info("ws disconnected when sendEvent SentenceBegin: {}", ex.toString());
        }
    }

    private void notifySentenceEnd(final WebSocket webSocket, final PayloadSentenceEnd payload) {
        try {
            if (webSocket.getAttachment() instanceof ASRSession session) {
                session.notifySentenceEnd(payload);
            }
            HubEventVO.sendEvent(webSocket, "SentenceEnd", payload);
        } catch (WebsocketNotConnectedException ex) {
            log.info("ws disconnected when sendEvent SentenceEnd: {}", ex.toString());
        }
    }

    private void notifyTranscriptionResultChanged(final WebSocket webSocket, final PayloadTranscriptionResultChanged payload) {
        try {
            HubEventVO.sendEvent(webSocket, "TranscriptionResultChanged", payload);
        } catch (WebsocketNotConnectedException ex) {
            log.info("ws disconnected when sendEvent TranscriptionResultChanged: {}", ex.toString());
        }
    }

    private void notifyTranscriptionCompleted(final WebSocket webSocket) {
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
        final ASRSession session = webSocket.getAttachment();
        if (session == null) {
            log.error("stopAndCloseTranscriber: {} without ASRSession, abort", webSocket.getRemoteSocketAddress());
            return;
        }

        session.stopAndCloseTranscriber();
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        _wsServer.stop();
        _nlsClient.shutdown();
        _txClient.shutdown();

        _nlsAuthExecutor.shutdownNow();
        _sessionExecutor.shutdownNow();
        _scheduledExecutor.shutdownNow();
        _ossAccessExecutor.shutdownNow();

        log.info("ASR-Hub: shutdown");
    }
}