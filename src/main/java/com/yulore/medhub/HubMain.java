package com.yulore.medhub;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nls.client.protocol.InputFormatEnum;
import com.alibaba.nls.client.protocol.NlsClient;
import com.alibaba.nls.client.protocol.OutputFormatEnum;
import com.alibaba.nls.client.protocol.SampleRateEnum;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriber;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriberListener;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriberResponse;
import com.aliyun.oss.OSS;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.primitives.Bytes;
import com.mgnt.utils.StringUnicodeEncoderDecoder;
import com.tencent.asrv2.*;
import com.tencent.core.ws.SpeechClient;
import com.yulore.bst.*;
import com.yulore.medhub.api.CallApi;
import com.yulore.medhub.api.CompositeVO;
import com.yulore.medhub.api.ScriptApi;
import com.yulore.medhub.nls.*;
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
public class HubMain {
    public static final byte[] EMPTY_BYTES = new byte[0];
    @Value("${ws_server.host}")
    private String _ws_host;

    @Value("${ws_server.port}")
    private int _ws_port;

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

//    @Value("${oss.endpoint}")
//    private String _oss_endpoint;
//
//    @Value("${oss.access_key_id}")
//    private String _oss_access_key_id;
//
//    @Value("${oss.access_key_secret}")
//    private String _oss_access_key_secret;

    @Value("${oss.bucket}")
    private String _oss_bucket;

    @Value("${oss.path}")
    private String _oss_path;

    //@Value("${oss.match_prefix}")
    //private String _oss_match_prefix;

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
    private NacosDiscoveryProperties _nacosDiscoveryProperties;

    @Autowired
    public HubMain(final OSS ossClient) {
        this._ossClient = ossClient;
    }

    @PostConstruct
    public void start() {
        //创建NlsClient实例应用全局创建一个即可。生命周期可和整个应用保持一致，默认服务地址为阿里云线上服务地址。
        _nlsClient = new NlsClient(_nls_url, "invalid_token");
        //_ossClient = new OSSClientBuilder().build(_oss_endpoint, _oss_access_key_id, _oss_access_key_secret);

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
                            log.error("handleASRCommand {}: {}, an error occurred when parseAsJson: {}",
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
        // registerToNacos(_wsServer.getPort());
    }

    private void registerToNacos(final String ip, final int port) {
        try {
            final NamingService namingService = _nacosDiscoveryProperties.namingServiceInstance();
            final Instance instance = new Instance();
            instance.setIp(ip);
            instance.setPort(port);
            instance.setHealthy(true);
            instance.setServiceName("med-hub");
            instance.setClusterName("DEFAULT");
            instance.getMetadata().put("preserved.register.source", "SPRING_CLOUD");

            namingService.registerInstance("med-hub", "DEFAULT_GROUP", instance);
            log.info("服务注册成功: {} - {}:{}, namespace={}", "med-hub", ip, port, _nacosDiscoveryProperties.getNamespace());
        } catch (final Exception ex) {
            log.warn("服务注册失败: {}", ex.getMessage());
        }
    }

    private void unregisterFromNacos(final String ip, final int port) {
        try {
            final NamingService namingService = _nacosDiscoveryProperties.namingServiceInstance();
            namingService.deregisterInstance("med-hub", "DEFAULT_GROUP", ip, port);
            log.info("服务反注册成功: {} - {}:{}, namespace={}", "med-hub", ip, port, _nacosDiscoveryProperties.getNamespace());
        } catch (final Exception ex) {
            log.warn("服务反注册失败: {}", ex.getMessage());
        }
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
            _sessionExecutor.submit(()-> handleStartTranscriptionCommand(cmd, webSocket));
        } else if ("StopTranscription".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleStopTranscriptionCommand(cmd, webSocket));
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
        return new CosyStreamTask(cvo2cosy(cvo), this::selectCosyAgent, (synthesizer) -> {
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

    /*
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

    private void handleStartTranscriptionCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        final String provider = cmd.getPayload() != null ? cmd.getPayload().get("provider") : null;
        final ASRActor session = webSocket.getAttachment();
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
                startWithTxasr(webSocket, session, cmd);
            } else {
                startWithAliasr(webSocket, session, cmd);
            }
        } catch (Exception ex) {
            // TODO: close websocket?
            log.error("StartTranscription: failed: {}", ex.toString());
            throw new RuntimeException(ex);
        } finally {
            session.unlock();
        }
    }

    private void startWithTxasr(final WebSocket webSocket, final ASRActor session, final WSCommandVO cmd) throws Exception {
        final long startConnectingInMs = System.currentTimeMillis();
        final TxASRAgent agent = selectTxASRAgent();

        final SpeechRecognizer speechRecognizer = buildSpeechRecognizer(agent,
                buildRecognizerListener(session, webSocket, agent, session.sessionId(), startConnectingInMs),
                (request)-> {
                    // https://cloud.tencent.com/document/product/1093/48982
                    if (cmd.getPayload().get("noise_threshold") != null) {
                        request.setNoiseThreshold(Float.parseFloat(cmd.getPayload().get("noise_threshold")));
                    }
                    if (cmd.getPayload().get("engine_model_type") != null) {
                        request.setEngineModelType(cmd.getPayload().get("engine_model_type"));
                    }
                    if (cmd.getPayload().get("input_sample_rate") != null) {
                        request.setInputSampleRate(Integer.parseInt(cmd.getPayload().get("input_sample_rate")));
                    }
                    if (cmd.getPayload().get("vad_silence_time") != null) {
                        request.setVadSilenceTime(Integer.parseInt(cmd.getPayload().get("vad_silence_time")));
                    }
                });

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

    private SpeechRecognizer buildSpeechRecognizer(final TxASRAgent agent, final SpeechRecognizerListener listener, final Consumer<SpeechRecognizerRequest> onRequest) throws Exception {
        //创建实例、建立连接。
        final SpeechRecognizer recognizer = agent.buildSpeechRecognizer(listener, onRequest);
        return recognizer;
    }

    private SpeechRecognizerListener buildRecognizerListener(final ASRActor session,
                                                             final WebSocket webSocket,
                                                             final TxASRAgent account,
                                                             final String sessionId,
                                                             final long startConnectingInMs) {
        // https://cloud.tencent.com/document/product/1093/48982
        return new SpeechRecognizerListener() {
            @Override
            public void onRecognitionStart(final SpeechRecognizerResponse response) {
                log.info("onRecognitionStart sessionId={},voice_id:{},{},cost={} ms",
                        sessionId,
                        response.getVoiceId(),
                        JSON.toJSONString(response),
                        System.currentTimeMillis() - startConnectingInMs);
                account.incConnected();
                notifyTranscriptionStarted(webSocket);
            }

            @Override
            public void onSentenceBegin(final SpeechRecognizerResponse response) {
                log.info("onSentenceBegin sessionId={},voice_id:{},{}",
                        sessionId,
                        response.getVoiceId(),
                        JSON.toJSONString(response));
                notifySentenceBegin(webSocket,
                        new PayloadSentenceBegin(response.getResult().getIndex(), response.getResult().getStartTime().intValue()));
            }

            @Override
            public void onSentenceEnd(final SpeechRecognizerResponse response) {
                log.info("onSentenceEnd sessionId={},voice_id:{},{}",
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
                log.info("onRecognitionResultChange sessionId={},voice_id:{},{}",
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
                log.info("onRecognitionComplete sessionId={},voice_id:{},{}",
                        sessionId,
                        response.getVoiceId(),
                        JSON.toJSONString(response));
                notifyTranscriptionCompleted(webSocket);
            }

            @Override
            public void onFail(final SpeechRecognizerResponse response) {
                log.warn("onFail sessionId={},voice_id:{},{}",
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

    private void startWithAliasr(final WebSocket webSocket, final ASRActor session, final WSCommandVO cmd) throws Exception {
        final long startConnectingInMs = System.currentTimeMillis();
        final ASRAgent agent = selectASRAgent();

        final SpeechTranscriber speechTranscriber = session.onSpeechTranscriberCreated(
                buildSpeechTranscriber(agent, buildTranscriberListener(session, webSocket, agent, session.sessionId(), startConnectingInMs)));

        if (cmd.getPayload() != null && cmd.getPayload().get("speech_noise_threshold") != null) {
            // ref: https://help.aliyun.com/zh/isi/developer-reference/websocket#sectiondiv-rz2-i36-2gv
            speechTranscriber.addCustomedParam("speech_noise_threshold", Float.parseFloat(cmd.getPayload().get("speech_noise_threshold")));
        }

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

    private SpeechTranscriberListener buildTranscriberListener(final ASRActor session,
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
                account.incConnected();
                notifyTranscriptionStarted(webSocket);
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

    private void notifyTranscriptionStarted(final WebSocket webSocket) {
        try {
            if (webSocket.getAttachment() instanceof ASRActor session) {
                session.transcriptionStarted();
            }
            WSEventVO.sendEvent(webSocket, "TranscriptionStarted", (Void) null);
        } catch (WebsocketNotConnectedException ex) {
            log.info("ws disconnected when sendEvent TranscriptionStarted: {}", ex.toString());
        }
    }

    private void notifySentenceBegin(final WebSocket webSocket, final PayloadSentenceBegin payload) {
        try {
            if (webSocket.getAttachment() instanceof ASRActor session) {
                session.notifySentenceBegin(payload);
            }
            WSEventVO.sendEvent(webSocket, "SentenceBegin", payload);
        } catch (WebsocketNotConnectedException ex) {
            log.info("ws disconnected when sendEvent SentenceBegin: {}", ex.toString());
        }
    }

    private void notifySentenceEnd(final WebSocket webSocket, final PayloadSentenceEnd payload) {
        try {
            if (webSocket.getAttachment() instanceof ASRActor session) {
                session.notifySentenceEnd(payload);
            }
            WSEventVO.sendEvent(webSocket, "SentenceEnd", payload);
        } catch (WebsocketNotConnectedException ex) {
            log.info("ws disconnected when sendEvent SentenceEnd: {}", ex.toString());
        }
    }

    private void notifyTranscriptionResultChanged(final WebSocket webSocket, final PayloadTranscriptionResultChanged payload) {
        try {
            if (webSocket.getAttachment() instanceof ASRActor session) {
                session.notifyTranscriptionResultChanged(payload);
            }
            WSEventVO.sendEvent(webSocket, "TranscriptionResultChanged", payload);
        } catch (WebsocketNotConnectedException ex) {
            log.info("ws disconnected when sendEvent TranscriptionResultChanged: {}", ex.toString());
        }
    }

    private void notifyTranscriptionCompleted(final WebSocket webSocket) {
        try {
            // TODO: account.dec??
            WSEventVO.sendEvent(webSocket, "TranscriptionCompleted", (Void)null);
        } catch (WebsocketNotConnectedException ex) {
            log.info("ws disconnected when sendEvent TranscriptionCompleted: {}", ex.toString());
        }
    }

    private void handleStopTranscriptionCommand(final WSCommandVO cmd, final WebSocket webSocket) {
        stopAndCloseTranscriber(webSocket);
        webSocket.close();
    }

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
        _nlsClient.shutdown();
        _txClient.shutdown();

        _nlsAuthExecutor.shutdownNow();
        _sessionExecutor.shutdownNow();
        _scheduledExecutor.shutdownNow();
        _ossAccessExecutor.shutdownNow();

        log.info("MediaHub: shutdown");
    }
}