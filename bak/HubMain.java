package com.yulore.medhub;

import com.alibaba.nls.client.protocol.OutputFormatEnum;
import com.alibaba.nls.client.protocol.SampleRateEnum;
import com.aliyun.oss.OSS;
import com.aliyun.oss.model.OSSObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.primitives.Bytes;
import com.mgnt.utils.StringUnicodeEncoderDecoder;
import com.yulore.bst.*;
import com.yulore.medhub.api.CallApi;
import com.yulore.medhub.api.CompositeVO;
import com.yulore.medhub.api.ScriptApi;
import com.yulore.medhub.nls.*;
import com.yulore.medhub.service.ASRService;
import com.yulore.medhub.service.TTSService;
import com.yulore.medhub.session.*;
import com.yulore.medhub.task.*;
import com.yulore.medhub.vo.*;
import com.yulore.util.ByteArrayListInputStream;
import com.yulore.util.ExceptionUtil;
import com.yulore.util.VarsUtil;
import com.yulore.util.WaveUtil;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.asynchttpclient.*;
import org.asynchttpclient.request.body.multipart.ByteArrayPart;
import org.asynchttpclient.request.body.multipart.StringPart;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
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
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@Slf4j
@Component
@ConditionalOnProperty(name = "ws_server.enabled", havingValue = "true")
public class HubMain {
    public static final byte[] EMPTY_BYTES = new byte[0];
    @Value("${ws_server.host}")
    private String _ws_host;

    @Value("${ws_server.port}")
    private int _ws_port;

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

    @Value("${call.answer_timeout_ms}")
    private long _answer_timeout_ms;

    @Value("${session.match_fs}")
    private String _match_fs;

    @Value("${session.match_media}")
    private String _match_media;

    @Value("${session.match_call}")
    private String _match_call;

    @Value("${session.match_playback}")
    private String _match_playback;

    @Value("${session.match_preview}")
    private String _match_preview;

    WebSocketServer _wsServer;

    private ExecutorService _sessionExecutor;

    private ScheduledExecutorService _scheduledExecutor;

    @Value("${oss.bucket}")
    private String _oss_bucket;

    @Value("${oss.path}")
    private String _oss_path;

    @Value("${rms.cp_prefix}")
    private String _rms_cp_prefix;

    @Value("${rms.tts_prefix}")
    private String _rms_tts_prefix;

    @Value("${rms.wav_prefix}")
    private String _rms_wav_prefix;

    @Value("${cosy2.url}")
    private String _cosy2_url;

    private final OSS _ossClient;

    private ExecutorService _ossAccessExecutor;

    private final AtomicInteger _currentWSConnection = new AtomicInteger(0);

    @Autowired
    private StreamCacheService _scsService;

    @Resource
    private ScriptApi _scriptApi;

    @Resource
    private CallApi _callApi;

    @Autowired
    private ASRService asrService;

    @Autowired
    private TTSService ttsService;

    @Autowired
    public HubMain(final OSS ossClient) {
        this._ossClient = ossClient;
    }

    @PostConstruct
    public void start() {

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
                        if (clientHandshake.getResourceDescriptor() != null && clientHandshake.getResourceDescriptor().startsWith(_match_fs)) {
                            // init FsSession attach with webSocket
                            final String role = clientHandshake.getFieldValue("x-role");
                            if ("asr".equals(role)) {
                                final String uuid = clientHandshake.getFieldValue("x-uuid");
                                final String sessionId = clientHandshake.getFieldValue("x-sessionid");
                                final String welcome = clientHandshake.getFieldValue("x-welcome");
                                final String recordStartTimestamp = clientHandshake.getFieldValue("x-rst");

                                final FsActor session = new FsActor(
                                        uuid,
                                        sessionId,
                                        _scriptApi,
                                        welcome,
                                        recordStartTimestamp,
                                        _rms_cp_prefix,
                                        _rms_tts_prefix,
                                        _rms_wav_prefix,
                                        _test_enable_delay,
                                        _test_delay_ms,
                                        _test_enable_disconnect,
                                        _test_disconnect_probability,
                                        ()->webSocket.close(1006, "test_disconnect"));
                                webSocket.setAttachment(session);
                                session.scheduleCheckIdle(_scheduledExecutor, _check_idle_interval_ms, session::checkIdle);
                                WSEventVO.<Void>sendEvent(webSocket, "FSConnected", null);
                                log.info("ws path match: {}, role: {}. using ws as FsSession {}", _match_fs, role, sessionId);
                            } else {
                                final String sessionId = clientHandshake.getFieldValue("x-sessionid");
                                log.info("onOpen: sessionid: {} for ws: {}", sessionId, webSocket.getRemoteSocketAddress());
                                final FsActor fs = FsActor.findBy(sessionId);
                                if (fs != null) {
                                    webSocket.setAttachment(fs);
                                    fs.attachPlaybackWs((event, payload) -> {
                                        try {
                                            WSEventVO.sendEvent(webSocket, event, payload);
                                        } catch (Exception ex) {
                                            log.warn("[{}]: FsActor sendback {}/{} failed, detail: {}", fs.sessionId(), event, payload, ex.toString());
                                        }
                                    });
                                    log.info("ws path match: {}, role: {}, attach exist FsSession {}", _match_fs, role, sessionId);
                                } else {
                                    log.warn("ws path match: {}, role: {}, !NOT! find FsSession with {}", _match_fs, role, sessionId);
                                }
                            }
                        } else if (clientHandshake.getResourceDescriptor() != null && clientHandshake.getResourceDescriptor().startsWith(_match_media)) {
                            // init MediaSession attach with webSocket
                            final String sessionId = clientHandshake.getFieldValue("x-sessionid");
                            final MediaSession session = new MediaSession(sessionId, _test_enable_delay, _test_delay_ms,
                                    _test_enable_disconnect, _test_disconnect_probability, ()->webSocket.close(1006, "test_disconnect"));
                            webSocket.setAttachment(session);
                            session.scheduleCheckIdle(_scheduledExecutor, _check_idle_interval_ms,
                                    ()-> WSEventVO.<Void>sendEvent(webSocket, "CheckIdle", null));
                            log.info("ws path match: {}, using ws as MediaSession {}", _match_media, sessionId);
                        } else if (clientHandshake.getResourceDescriptor() != null && clientHandshake.getResourceDescriptor().startsWith(_match_call)) {
                            // init PoActor attach with webSocket
                            final String path = clientHandshake.getResourceDescriptor();
                            final int varsBegin = path.indexOf('?');
                            final String uuid = varsBegin > 0 ? VarsUtil.extractValueWithSplitter(path.substring(varsBegin + 1), "uuid", '&') : "unknown";
                            final String tid = varsBegin > 0 ? VarsUtil.extractValueWithSplitter(path.substring(varsBegin + 1), "tid", '&') : "unknown";
                            final String clientIp = clientHandshake.getFieldValue("X-Forwarded-For");
                            final PoActor actor = new PoActor(
                                    clientIp,
                                    uuid,
                                    tid,
                                    _callApi,
                                    _scriptApi,
                                    (_session)->{
                                        try {
                                            WSEventVO.sendEvent(webSocket, "CallEnded", new PayloadCallEnded(_session.sessionId()));
                                            log.info("[{}]: sendback CallEnded event", _session.sessionId());
                                        } catch (Exception ex) {
                                            log.warn("[{}]: sendback CallEnded event failed, detail: {}", _session.sessionId(), ex.toString());
                                        }
                                    },
                                    _oss_bucket,
                                    _oss_path,
                                    (ctx) -> {
                                        final long startUploadInMs = System.currentTimeMillis();
                                        _ossAccessExecutor.submit(()->{
                                            _ossClient.putObject(ctx.bucketName(), ctx.objectName(), ctx.content());
                                            log.info("[{}]: upload record to oss => bucket:{}/object:{}, cost {} ms",
                                                    ctx.sessionId(), ctx.bucketName(), ctx.objectName(), System.currentTimeMillis() - startUploadInMs);
                                        });
                                    },
                                    (sessionId) -> WSEventVO.sendEvent(webSocket, "CallStarted", new PayloadCallStarted(sessionId)));
                            webSocket.setAttachment(actor);
                            actor.scheduleCheckIdle(_scheduledExecutor, _check_idle_interval_ms, actor::checkIdle);
                            _scheduledExecutor.schedule(actor::notifyMockAnswer, _answer_timeout_ms, TimeUnit.MILLISECONDS);

                            log.info("ws path match: {}, using ws as PoActor", _match_call);
                        } else if (clientHandshake.getResourceDescriptor() != null && clientHandshake.getResourceDescriptor().startsWith(_match_playback)) {
                            // init PlaybackSession attach with webSocket
                            final String path = clientHandshake.getResourceDescriptor();
                            final int varsBegin = path.indexOf('?');
                            final String sessionId = varsBegin > 0 ? VarsUtil.extractValue(path.substring(varsBegin + 1), "sessionId") : "unknown";
                            // final PlaybackActor playbackSession = new PlaybackActor(sessionId);
                            webSocket.setAttachment(sessionId);
                            log.info("ws path match: {}, using ws as PoActor's playback ws: [{}]", _match_playback, sessionId);
                            final PoActor poActor = PoActor.findBy(sessionId);
                            if (poActor == null) {
                                log.info("can't find callSession by sessionId: {}, ignore", sessionId);
                                return;
                            }
                            poActor.attachPlaybackWs(
                                    (playbackContext) ->
                                            playbackOn2(playbackContext,
                                                    poActor,
                                                    webSocket),
                                    (event, payload) -> {
                                        try {
                                            WSEventVO.sendEvent(webSocket, event, payload);
                                        } catch (Exception ex) {
                                            log.warn("[{}]: PoActor sendback {}/{} failed, detail: {}", poActor.sessionId(), event, payload, ex.toString());
                                        }
                                    });
                        } else if (clientHandshake.getResourceDescriptor() != null && clientHandshake.getResourceDescriptor().startsWith(_match_preview)) {
                            // init PreviewSession attach with webSocket
                            final PreviewSession previewSession = new PreviewSession();
                            webSocket.setAttachment(previewSession);
                            log.info("ws path match: {}, using ws as PreviewSession: [{}]", _match_preview, previewSession);
                        } else {
                            log.info("ws path {} !NOT! match: {}, NOT MediaSession: {}",
                                    clientHandshake.getResourceDescriptor(), _match_media, webSocket.getRemoteSocketAddress());
                        }
                    }

                    @Override
                    public void onClose(final WebSocket webSocket, final int code, final String reason, final boolean remote) {
                        final Object attachment = webSocket.getAttachment();
                        if (attachment instanceof FsActor session) {
                            stopAndCloseTranscriber(webSocket);
                            session.close();
                            log.info("wscount/{}: closed {} with exit code {} additional info: {}, FsSession-id {}",
                                    _currentWSConnection.decrementAndGet(),
                                    webSocket.getRemoteSocketAddress(), code, reason, session.sessionId());
                        } else if (attachment instanceof MediaSession session) {
                            stopAndCloseTranscriber(webSocket);
                            session.close();
                            log.info("wscount/{}: closed {} with exit code {} additional info: {}, MediaSession-id {}",
                                    _currentWSConnection.decrementAndGet(),
                                    webSocket.getRemoteSocketAddress(), code, reason, session.sessionId());
                        } else if (attachment instanceof PoActor session) {
                            stopAndCloseTranscriber(webSocket);
                            session.close();
                            log.info("wscount/{}: closed {} with exit code {} additional info: {}, CallSession-id: {}",
                                    _currentWSConnection.decrementAndGet(),
                                    webSocket.getRemoteSocketAddress(), code, reason, session.sessionId());
                        } else if (attachment instanceof String sessionId) {
                            log.info("wscount/{}: closed {} with exit code {} additional info: {}, ws's peer PoActor-id: {}",
                                    _currentWSConnection.decrementAndGet(),
                                    webSocket.getRemoteSocketAddress(), code, reason, sessionId);
                        } else if (attachment instanceof PreviewSession session) {
                            log.info("wscount/{}: closed {} with exit code {} additional info: {}, PreviewSession",
                                    _currentWSConnection.decrementAndGet(),
                                    webSocket.getRemoteSocketAddress(), code, reason);
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
                            handleHubCommand(new ObjectMapper().readValue(message, WSCommandVO.class), webSocket);
                        } catch (JsonProcessingException ex) {
                            log.error("handleHubCommand {}: {}, an error occurred when parseAsJson: {}",
                                    webSocket.getRemoteSocketAddress(), message, ex.toString());
                        }
                    }

                    @Override
                    public void onMessage(final WebSocket webSocket, final ByteBuffer bytes) {
                        final Object attachment = webSocket.getAttachment();
                        if (attachment instanceof ASRActor session) {
                            handleASRData(bytes, session);
                            return;
                        } else if (attachment instanceof StreamSession session) {
                            _sessionExecutor.submit(()-> handleFileWriteCommand(bytes, session, webSocket));
                            return;
                        }
                        log.error("onMessage(Binary): {} without any Session, ignore", webSocket.getRemoteSocketAddress());

                    }

                    @Override
                    public void onError(final WebSocket webSocket, final Exception ex) {
                        final Object attachment = webSocket.getAttachment();
                        if (attachment instanceof PoActor actor) {
                            actor.onWebsocketError(ex);
                            return;
                        }
                        log.warn("an error occurred on connection {}:{}",
                                webSocket.getRemoteSocketAddress(), ExceptionUtil.exception2detail(ex));
                    }

                    @Override
                    public void onStart() {
                        log.info("server started successfully");
                    }
                };
        // _wsServer.setConnectionLostTimeout(_ws_heartbeat);
        _wsServer.start();
    }

    private void playbackOn(final String path, final String contentId, final PoActor callSession, final PlaybackActor playbackSession, final WebSocket webSocket) {
        // interval = 20 ms
        int interval = 20;
        log.info("[{}]: playbackOn: {} => sample rate: {}/interval: {}/channels: {}", callSession.sessionId(), path, 16000, interval, 1);
        final PlayStreamPCMTask task = new PlayStreamPCMTask(
                callSession.sessionId(),
                path,
                _scheduledExecutor,
                new SampleInfo(16000, interval, 16, 1),
                (timestamp) -> callSession.notifyPlaybackSendStart(contentId, timestamp),
                (timestamp) -> callSession.notifyPlaybackSendStop(contentId, timestamp),
                (bytes) -> {
                    webSocket.send(bytes);
                    callSession.notifyPlaybackSendData(bytes);
                },
                (_task) -> {
                    log.info("[{}]: PlayStreamPCMTask {} stopped with completed: {}", callSession.sessionId(), _task, _task.isCompleted());
                    callSession.notifyPlaybackStop(_task.taskId(), contentId, null, null, null);
                    playbackSession.notifyPlaybackStop(_task);
                }
        );
        final BuildStreamTask bst = getTaskOf(path, true, 16000);
        if (bst != null) {
            playbackSession.attach(task);
            callSession.notifyPlaybackStart(task.taskId());
            playbackSession.notifyPlaybackStart(task);
            bst.buildStream(task::appendData, (ignore)->task.appendCompleted());
        }
    }

    private void handlePCMPlaybackStartedCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        // eg: {"header": {"name": "PCMPlaybackStarted"},"payload": {"playback_id": "ebfafab3-eb5e-454a-9427-187ceff9ff23", "content_id": "2213745"}}
        final String playbackId = cmd.getPayload() != null ? cmd.getPayload().get("playback_id") : null;
        final String contentId = cmd.getPayload() != null ? cmd.getPayload().get("content_id") : null;

        final Object attachment = webSocket.getAttachment();
        if (attachment instanceof String sessionId) {
            PoActor poActor = PoActor.findBy(sessionId);
            log.info("[{}]: handlePCMPlaybackStartedCommand: playbackId: {}/attached PoActor: {}", sessionId, playbackId, poActor != null);
            if (poActor != null) {
                poActor.notifyPlaybackStarted(playbackId, contentId);
            }
        }
    }

    private void handlePCMPlaybackResumedCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        // eg: {"header": {"name": "PCMPlaybackResumed"},"payload": {"playback_id": "ebfafab3-eb5e-454a-9427-187ceff9ff23", "content_id": "2213745", "playback_duration": "4.410666666666666"}}
        final String playbackId = cmd.getPayload() != null ? cmd.getPayload().get("playback_id") : null;
        final String contentId = cmd.getPayload() != null ? cmd.getPayload().get("content_id") : null;
        final String playback_duration  = cmd.getPayload() != null ? cmd.getPayload().get("playback_duration") : null; //"4.410666666666666"

        final Object attachment = webSocket.getAttachment();
        if (attachment instanceof String sessionId) {
            PoActor poActor = PoActor.findBy(sessionId);
            log.info("[{}]: handlePCMPlaybackResumedCommand: playbackId: {}/attached PoActor: {}", sessionId, playbackId, poActor != null);
            if (poActor != null) {
                poActor.notifyPlaybackResumed(playbackId, contentId, playback_duration);
            }
        }
    }

    private void handlePCMPlaybackPausedCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        // eg: {"header": {"name": "PCMPlaybackPaused"},"payload": {"playback_id": "ebfafab3-eb5e-454a-9427-187ceff9ff23", "content_id": "2213745", "playback_duration": "4.410666666666666"}}
        final String playbackId = cmd.getPayload() != null ? cmd.getPayload().get("playback_id") : null;
        final String contentId = cmd.getPayload() != null ? cmd.getPayload().get("content_id") : null;
        final String playback_duration  = cmd.getPayload() != null ? cmd.getPayload().get("playback_duration") : null; //"4.410666666666666"

        final Object attachment = webSocket.getAttachment();
        if (attachment instanceof String sessionId) {
            PoActor poActor = PoActor.findBy(sessionId);
            log.info("[{}]: handlePCMPlaybackPausedCommand: playbackId: {}/attached PoActor: {}", sessionId, playbackId, poActor != null);
            if (poActor != null) {
                poActor.notifyPlaybackPaused(playbackId, contentId, playback_duration);
            }
        }
    }

    private void handlePCMPlaybackStoppedCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        final String playbackId = cmd.getPayload() != null ? cmd.getPayload().get("playback_id") : null;
        final String contentId = cmd.getPayload() != null ? cmd.getPayload().get("content_id") : null;
        final String playback_begin_timestamp = cmd.getPayload() != null ? cmd.getPayload().get("playback_begin_timestamp") : null;
        final String playback_end_timestamp  = cmd.getPayload() != null ? cmd.getPayload().get("playback_end_timestamp") : null;//": "1736389958856"
        final String playback_duration  = cmd.getPayload() != null ? cmd.getPayload().get("playback_duration") : null; //": "3.4506666666666668"}

        final Object attachment = webSocket.getAttachment();
        if (attachment instanceof String sessionId) {
            PoActor poActor = PoActor.findBy(sessionId);
            log.info("[{}]: handlePCMPlaybackStoppedCommand: playbackId: {}/attached PoActor: {}", sessionId, playbackId, poActor != null);
            if (poActor != null) {
                poActor.notifyPlaybackStop(playbackId, contentId, playback_begin_timestamp, playback_end_timestamp, playback_duration);
            }
        }
    }

    private Runnable playbackOn2(final PoActor.PlaybackContext playbackContext, final PoActor poActor, final WebSocket webSocket) {
        // interval = 20 ms
        int interval = 20;
        log.info("[{}]: playbackOn2: {} => sample rate: {}/interval: {}/channels: {} as {}",
                poActor.sessionId(), playbackContext.path(), 16000, interval, 1, playbackContext.playbackId());
        final PlayStreamPCMTask2 task = new PlayStreamPCMTask2(
                playbackContext.playbackId(),
                poActor.sessionId(),
                playbackContext.path(),
                _scheduledExecutor,
                new SampleInfo(16000, interval, 16, 1),
                (timestamp) -> {
                    WSEventVO.sendEvent(webSocket, "PCMBegin",
                            new PayloadPCMEvent(playbackContext.playbackId(), playbackContext.contentId()));
                    poActor.notifyPlaybackSendStart(playbackContext.playbackId(), timestamp);
                },
                (timestamp) -> {
                    poActor.notifyPlaybackSendStop(playbackContext.playbackId(), timestamp);
                    WSEventVO.sendEvent(webSocket, "PCMEnd",
                            new PayloadPCMEvent(playbackContext.playbackId(), playbackContext.contentId()));
                },
                (bytes) -> {
                    webSocket.send(bytes);
                    poActor.notifyPlaybackSendData(bytes);
                },
                (_task) -> {
                    log.info("[{}]: PlayStreamPCMTask2 {} send data stopped with completed: {}",
                            poActor.sessionId(), _task, _task.isCompleted());
                }
        );

        final BuildStreamTask bst = getTaskOf(playbackContext.path(), true, 16000);
        if (bst != null) {
            poActor.notifyPlaybackStart(playbackContext.playbackId());
            bst.buildStream(task::appendData, (ignore)->task.appendCompleted());
        }
        return task::stop;
    }

    private void handleASRData(final ByteBuffer bytes, final ASRActor session) {
        if (session.transmit(bytes)) {
            // transmit success
            if ((session.transmitCount() % 50) == 0) {
                log.debug("{}: transmit 50 times.", session.sessionId());
            }
        }
    }

    private void handleHubCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        if ("StartTranscription".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> asrService.startTranscription(cmd, webSocket));
        } else if ("StopTranscription".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> asrService.stopTranscription(cmd, webSocket));
        } else if ("FSPlaybackStarted".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleFSPlaybackStartedCommand(cmd, webSocket));
        } else if ("FSPlaybackStopped".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleFSPlaybackStoppedCommand(cmd, webSocket));
        } else if ("FSPlaybackPaused".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleFSPlaybackPausedCommand(cmd, webSocket));
        } else if ("FSPlaybackResumed".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleFSPlaybackResumedCommand(cmd, webSocket));
        } else if ("FSRecordStarted".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleFSRecordStartedCommand(cmd, webSocket));
        } else if ("PCMPlaybackStopped".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handlePCMPlaybackStoppedCommand(cmd, webSocket));
        } else if ("PCMPlaybackPaused".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handlePCMPlaybackPausedCommand(cmd, webSocket));
        } else if ("PCMPlaybackResumed".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handlePCMPlaybackResumedCommand(cmd, webSocket));
        } else if ("PCMPlaybackStarted".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handlePCMPlaybackStartedCommand(cmd, webSocket));
        } /* else if ("Playback".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handlePlaybackCommand(cmd, webSocket));
        } else if ("PlayTTS".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handlePlayTTSCommand(cmd, webSocket));
        } else if ("StopPlayback".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleStopPlaybackCommand(cmd, webSocket));
        } else if ("PausePlayback".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handlePausePlaybackCommand(cmd, webSocket));
        } else if ("ResumePlayback".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleResumePlaybackCommand(cmd, webSocket));
        } */ else if ("OpenStream".equals(cmd.getHeader().get("name"))) {
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
        } else if ("Preview".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handlePreviewCommand(cmd, webSocket));
        } else if ("Cosy2".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleCosy2Command(cmd, webSocket));
        } else {
            log.warn("handleHubCommand: Unknown Command: {}", cmd);
        }
    }

    private void handleFSRecordStartedCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        final Object attachment = webSocket.getAttachment();
        if (attachment instanceof FsActor session) {
            session.notifyFSRecordStarted(cmd);
        }
    }

    private void handleFSPlaybackStartedCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        final Object attachment = webSocket.getAttachment();
        if (attachment instanceof FsActor session) {
            session.notifyFSPlaybackStarted(cmd);
        }
    }

    private void handleFSPlaybackStoppedCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        final Object attachment = webSocket.getAttachment();
        if (attachment instanceof FsActor session) {
            session.notifyFSPlaybackStopped(cmd);
        }
    }

    private void handleFSPlaybackResumedCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        final Object attachment = webSocket.getAttachment();
        if (attachment instanceof FsActor fsActor) {
            fsActor.notifyPlaybackResumed(cmd);
        }
    }

    private void handleFSPlaybackPausedCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        final Object attachment = webSocket.getAttachment();
        if (attachment instanceof FsActor fsActor) {
            fsActor.notifyPlaybackPaused(cmd);
        }
    }

    private void handleCosy2Command(final WSCommandVO cmd, final WebSocket webSocket) {
        final String ttsText = cmd.getPayload().get("tts_text");
        final String promptText = cmd.getPayload().get("prompt_text");

        // {bucket=xxxx}yyyy/zzzz.wav
        final String promptWav = cmd.getPayload().get("prompt_wav");

        final BuildStreamTask bst = new OSSStreamTask(promptWav, _ossClient, false);
        final List<byte[]> byteList = new ArrayList<>();
        bst.buildStream(byteList::add, (ignored) -> {
            try (final InputStream is = new ByteArrayListInputStream(byteList);
            final ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
                is.transferTo(bos);
                final byte[] promptWavBytes = bos.toByteArray();
                log.info("Cosy2: ttsText:{}/promptText:{}/promptWav size:{}", ttsText, promptText, promptWavBytes.length);
                final byte[] wavBytes = callCosy2ZeroShot(ttsText, promptText, promptWavBytes);
                if (wavBytes != null) {
                    webSocket.send(wavBytes);
                    log.info("Cosy2: output wav size:{}", wavBytes.length);
                } else {
                    log.info("Cosy2: output wav failed");
                }
            } catch (IOException ignored1) {
            }
        });
    }

    private byte[] callCosy2ZeroShot(final String ttsText, final String promptText, final byte[] wavBytes) {
        try {
            // 创建 HttpClient 实例
            final HttpClient httpClient = HttpClients.createDefault();

            // 创建 HttpGet 请求
            // HttpGet httpGet = new HttpGet(_cosy2_url);
            final HttpPost httpPost = new HttpPost(_cosy2_url);

            // 构建 MultipartEntity
            final HttpEntity entity = MultipartEntityBuilder.create()
                    .setCharset(StandardCharsets.UTF_8) // 设置全局字符编码为 UTF-8
                    .addTextBody("tts_text", ttsText, ContentType.TEXT_PLAIN.withCharset(StandardCharsets.UTF_8))
                    .addTextBody("prompt_text", promptText, ContentType.TEXT_PLAIN.withCharset(StandardCharsets.UTF_8))
                    .addBinaryBody("prompt_wav", wavBytes, ContentType.APPLICATION_OCTET_STREAM, "prompt_wav")
                    .build();

            // 设置请求体
            httpPost.setEntity(entity);

            // 发送请求并获取响应
            HttpResponse response = httpClient.execute(httpPost);

            // 检查响应状态码
            if (response.getStatusLine().getStatusCode() == 200) {
                // 保存响应内容到文件
                HttpEntity responseEntity = response.getEntity();
                if (responseEntity != null) {
                    byte[] pcm = EntityUtils.toByteArray(responseEntity);
                    log.info("callCosy2ZeroShot: Response {}/audioData.size: {}", response, pcm.length);
                    return Bytes.concat(
                            WaveUtil.genWaveHeader(16000, 1),
                            resamplePCM(pcm, 24000, 16000));
                }
            } else {
                log.warn("Failed to get response. Status code: {}", response.getStatusLine().getStatusCode());
            }
        } catch (IOException ex) {
            log.warn("callCosy2ZeroShot: failed, detail: {}", ExceptionUtil.exception2detail(ex));
        }
        return null;
    }

    // 转换采样率
    private static byte[] resamplePCM(final byte[] pcm, final int sourceSampleRate, final int targetSampleRate) {
        final AudioFormat sf = new AudioFormat(sourceSampleRate, 16, 1, true, false);
        final AudioFormat tf = new AudioFormat(targetSampleRate, 16, 1, true, false);
        try(// 创建原始音频格式
            final AudioInputStream ss = new AudioInputStream(new ByteArrayInputStream(pcm), sf, pcm.length);
            // 创建目标音频格式
            final AudioInputStream ts = AudioSystem.getAudioInputStream(tf, ss);
            // 读取转换后的数据
            final ByteArrayOutputStream os = new ByteArrayOutputStream();
        ) {
            ts.transferTo(os);
            return os.toByteArray();
        } catch (Exception ex) {
            log.warn("resamplePCM: failed, detail: {}", ExceptionUtil.exception2detail(ex));
            return null;
        }
    }

    private void callCosy2InferenceZeroShot(final String ttsText, final String promptText, final byte[] wavBytes) {
        final String outputPath = "response.wav";

        log.info("callCosy2InferenceZeroShot: Step0");
        try(final AsyncHttpClient ahc = Dsl.asyncHttpClient(new DefaultAsyncHttpClientConfig.Builder()
                     .setMaxRequestRetry(0)
                    .setHttpClientCodecMaxChunkSize(10240000)
                    .setWebSocketMaxBufferSize(1024000)
                    .setWebSocketMaxFrameSize(1024000)
                    .build())) {
            log.info("callCosy2InferenceZeroShot: Step1");

            final BoundRequestBuilder requestBuilder = ahc.prepareGet(_cosy2_url);

            final ByteArrayPart part3 = new ByteArrayPart("prompt_wav", wavBytes, "application/octet-stream");

            log.info("callCosy2InferenceZeroShot: Step2");
            // 构建 Multipart 请求体
            requestBuilder
                    .setHeader("Transfer-Encoding", "chunked")
                    .addBodyPart(new StringPart("tts_text", ttsText))
                    .addBodyPart(new StringPart("prompt_text", promptText))
                    .addBodyPart(part3)
                    ;
                    //.build();

            log.info("callCosy2InferenceZeroShot: Step3");

            final Future<Response> responseFuture = requestBuilder.execute();

            log.info("callCosy2InferenceZeroShot: Step4");

            // 获取响应
            final Response response = responseFuture.get();

            log.info("callCosy2InferenceZeroShot: Step5");

            // 检查响应状态码
            if (response.getStatusCode() == 200) {
                // 保存响应内容到文件
                byte[] audioData = response.getResponseBodyAsBytes();
                // saveToFile(audioData, outputPath);
                log.info("callCosy2InferenceZeroShot: Response {}", response);
            } else {
                log.warn("callCosy2InferenceZeroShot: Failed to get response. Status code: {}", response.getStatusCode());
            }
        } catch (Exception ex) {
            log.warn("callCosy2InferenceZeroShot: failed, detail: {}", ExceptionUtil.exception2detail(ex));
            // throw new RuntimeException(e);
        }
    }

    private void previewOn(final String path, final PreviewSession previewSession, final WebSocket webSocket) {
        // interval = 20 ms
        int interval = 20;
        log.info("previewOn: {} => sample rate: {}/interval: {}/channels: {}", path, 16000, interval, 1);
        final PlayStreamPCMTask task = new PlayStreamPCMTask(
                "preview",
                path,
                _scheduledExecutor,
                new SampleInfo(16000, interval, 16, 1),
                (ignore)->{},
                (ignore)->{},
                webSocket::send,
                (_task) -> {
                    log.info("previewOn PlayStreamPCMTask {} stopped with completed: {}", _task, _task.isCompleted());
                    webSocket.close(1000, "close");
                }
        );
        final BuildStreamTask bst = getTaskOf(path, true, 16000);
        if (bst != null) {
            previewSession.attach(task);
            previewSession.notifyPlaybackStart(task);
            bst.buildStream(task::appendData, (ignore)->task.appendCompleted());
        }
    }

    private void handlePreviewCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        final String cps = URLDecoder.decode(cmd.getPayload().get("cps"), Charsets.UTF_8);
        final PreviewSession previewSession = webSocket.getAttachment();
        if (previewSession == null) {
            log.error("Preview: {} without PreviewSession, abort: {}", webSocket.getRemoteSocketAddress(), cps);
            return;
        }

        if (previewSession.isPlaying()) {
            log.error("Preview: {} has already playing, ignore: {}", previewSession, cps);
            return;
        }

        previewOn(String.format("type=cp,%s", cps), previewSession, webSocket);
    }

    private void handleUserAnswerCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        final PoActor session = webSocket.getAttachment();
        if (session == null) {
            log.error("UserAnswer: {} without CallSession, abort", webSocket.getRemoteSocketAddress());
            return;
        }
        session.notifyUserAnswer(cmd);
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
                final BuildStreamTask bst = new TTSStreamTask(path, ttsService::selectTTSAgentAsync, (synthesizer) -> {
                    synthesizer.setFormat(removeWavHdr ? OutputFormatEnum.PCM : OutputFormatEnum.WAV);
                    synthesizer.setSampleRate(sampleRate);
                });
                return bst.key() != null ? _scsService.asCache(bst) : bst;
            } else if (path.contains("type=cosy")) {
                final BuildStreamTask bst = new CosyStreamTask(path, ttsService::selectCosyAgentAsync, (synthesizer) -> {
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
        return new CosyStreamTask(cvo2cosy(cvo), ttsService::selectCosyAgentAsync, (synthesizer) -> {
            //设置返回音频的编码格式
            synthesizer.setFormat(OutputFormatEnum.PCM);
            //设置返回音频的采样率。
            synthesizer.setSampleRate(SampleRateEnum.SAMPLE_RATE_16K);
        });
    }

    private BuildStreamTask genTtsStreamTask(final CompositeVO cvo) {
        return new TTSStreamTask(cvo2tts(cvo), ttsService::selectTTSAgentAsync, (synthesizer) -> {
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

    /*
    private void handlePlayTTSCommand(final WSCommandVO cmd, final WebSocket webSocket) {
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
        final TTSAgent agent = ttsService.selectTTSAgent();
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

    private void handlePlaybackCommand(final WSCommandVO cmd, final WebSocket webSocket) {
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

    private void handleStopPlaybackCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        final MediaSession session = webSocket.getAttachment();
        if (session == null) {
            log.error("StopPlayback: {} without Session, abort", webSocket.getRemoteSocketAddress());
            return;
        }
        log.info("[{}]: handleStopPlaybackCommand", session.sessionId());
        session.stopCurrentAnyway();
    }

    private void handlePausePlaybackCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        final MediaSession session = webSocket.getAttachment();
        if (session == null) {
            log.error("PausePlayback: {} without Session, abort", webSocket.getRemoteSocketAddress());
            return;
        }
        log.info("[{}]: handlePausePlaybackCommand", session.sessionId());
        session.pauseCurrentAnyway();
    }

    private void handleResumePlaybackCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        final MediaSession session = webSocket.getAttachment();
        if (session == null) {
            log.error("ResumePlayback: {} without Session, abort", webSocket.getRemoteSocketAddress());
            return;
        }
        log.info("[{}]: handleResumePlaybackCommand", session.sessionId());
        session.resumeCurrentAnyway();
    }

    private void playbackByFile(final String file, final WSCommandVO cmd, final MediaSession session, final WebSocket webSocket) {
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

    private void playbackById(final int id, final WSCommandVO cmd, final MediaSession session, final WebSocket webSocket) {
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
     */

    private static void stopAndCloseTranscriber(final WebSocket webSocket) {
        final ASRActor session = webSocket.getAttachment();
        if (session == null) {
            log.error("stopAndCloseTranscriber: {} without ASRSession, abort", webSocket.getRemoteSocketAddress());
            return;
        }

        session.stopAndCloseTranscriber();
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        _wsServer.stop();

        _sessionExecutor.shutdownNow();
        _scheduledExecutor.shutdownNow();
        _ossAccessExecutor.shutdownNow();

        log.info("MediaHub: shutdown");
    }
}