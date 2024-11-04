package com.yulore.medhub;

import com.alibaba.nls.client.AccessToken;
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
import com.yulore.medhub.nls.ASRAgent;
import com.yulore.medhub.nls.TTSAgent;
import com.yulore.medhub.nls.TTSTask;
import com.yulore.medhub.session.MediaSession;
import com.yulore.medhub.vo.*;
import com.yulore.l16.L16File;
import com.yulore.util.ByteArrayListInputStream;
import io.netty.util.NettyRuntime;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.jetbrains.annotations.NotNull;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

// 20241103: asr-hub rename to med-hub (Media Hub)

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
@Slf4j
@Component
public class HubMain {
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

    @Value("${test.enable_delay}")
    private boolean _test_enable_delay;

    @Value("${test.delay_ms}")
    private long _test_delay_ms;

    final List<ASRAgent> _asrAgents = new ArrayList<>();
    final List<TTSAgent> _ttsAgents = new ArrayList<>();

    private ScheduledExecutorService _nlsAuthExecutor;
    
    WebSocketServer _wsServer;

    private NlsClient _nlsClient;

    private ExecutorService _sessionExecutor;

    private ConcurrentMap<String, L16File> _id2L16File = new ConcurrentHashMap<>();

    private ScheduledExecutorService _playbackExecutor;

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

    @Value("${l16.path}")
    private String _l16_path;

    @PostConstruct
    public void start() {
        //创建NlsClient实例应用全局创建一个即可。生命周期可和整个应用保持一致，默认服务地址为阿里云线上服务地址。
        _nlsClient = new NlsClient(_nls_url, "invalid_token");
        _ossClient = new OSSClientBuilder().build(_oss_endpoint, _oss_access_key_id, _oss_access_key_secret);

        initNlsAgents(_nlsClient);

        _ossDownloader = Executors.newFixedThreadPool(NettyRuntime.availableProcessors() * 2);
        _sessionExecutor = Executors.newFixedThreadPool(NettyRuntime.availableProcessors() * 2);
        _playbackExecutor = Executors.newScheduledThreadPool(NettyRuntime.availableProcessors() * 2);

        _wsServer = new WebSocketServer(new InetSocketAddress(_ws_host, _ws_port)) {
                    @Override
                    public void onOpen(final WebSocket webSocket, final ClientHandshake clientHandshake) {
                        log.info("new connection to {}", webSocket.getRemoteSocketAddress());
                        // init session attach with webSocket
                        webSocket.setAttachment(new MediaSession(_test_enable_delay, _test_delay_ms));
                    }

                    @Override
                    public void onClose(WebSocket webSocket, int code, String reason, boolean remote) {
                        log.info("closed {} with exit code {} additional info: {}", webSocket.getRemoteSocketAddress(), code, reason);
                        stopAndCloseTranscriber(webSocket);
                        webSocket.<MediaSession>getAttachment().close();
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
        log.info("asr agent init, count:{}", _asrAgents.size());

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

        _nlsAuthExecutor = Executors.newSingleThreadScheduledExecutor();
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

    private void checkAndUpdateNlsToken() {
        for (ASRAgent agent : _asrAgents) {
            agent.checkAndUpdateAccessToken();
        }
        for (TTSAgent agent : _ttsAgents) {
            agent.checkAndUpdateAccessToken();
        }
    }

    private void handleASRData(final ByteBuffer bytes, final WebSocket webSocket) {
        final MediaSession session = webSocket.getAttachment();
        if (session == null) {
            log.error("handleASRData: {} without Session, abort", webSocket.getRemoteSocketAddress());
            return;
        }
        session.transmit(bytes);
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
        } else {
            log.warn("handleHubCommand: Unknown Command: {}", cmd);
        }
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
        if (!session.startPlaying()) {
            log.error("PlayTTS: {} playing another, abort this play command", webSocket.getRemoteSocketAddress());
            return;
        }

        final List<byte[]> bufs = new ArrayList<>();
        final long startInMs = System.currentTimeMillis();
        final TTSAgent agent = selectTTSAgent();
        final AtomicInteger idx = new AtomicInteger(0);
        final TTSTask task = new TTSTask(agent, text,
                (bytes) -> {
                    final byte[] bytesArray = new byte[bytes.remaining()];
                    bytes.get(bytesArray, 0, bytesArray.length);
                    bufs.add(bytesArray);
                    log.info("{}: onData {} bytes", idx.incrementAndGet(), bytesArray.length);
                },
                (response)->playPcmTo(new ByteArrayListInputStream(bufs), webSocket, startInMs, session::stopPlaying),
                (response)->{
                    log.warn("tts failed: {}", response);
                    session.stopPlaying();
                });
        task.start();
    }

    private void playPcmTo(final InputStream is, final WebSocket webSocket, final long startInMs, final Runnable onEnd) {
        log.info("playPcmTo: gen pcm stream cost={} ms", System.currentTimeMillis() - startInMs);
        sendEvent(webSocket, "PlaybackStart",
                new PayloadPlaybackStart("tts", 8000, 20, 1));
        final List<ScheduledFuture<?>> futures = new ArrayList<>();
        long delay = 20;
        int idx = 0;
        try (is) {
            while (true) {
                final byte[] bytes = new byte[320];
                final int readSize = is.read(bytes);
                log.info("{}: playPcmTo read {} bytes", ++idx, readSize);
                if (readSize == 320) {
                    final ScheduledFuture<?> future = _playbackExecutor.schedule(() -> webSocket.send(bytes), delay, TimeUnit.MILLISECONDS);
                    futures.add(future);
                    delay += 20;
                } else {
                    break;
                }
            }
        } catch (IOException ex) {
            log.warn("playPcmTo: {}", ex.toString());
        }

        futures.add(_playbackExecutor.schedule(()->{
                    sendEvent(webSocket, "PlaybackStop", new PayloadPlaybackStop("tts"));
                    onEnd.run();
                },
                delay, TimeUnit.MILLISECONDS));
        log.info("playPcmTo: schedule tts by {} send action", futures.size());
    }

            /*
            final int begin = file.lastIndexOf('/');
            final int end = file.indexOf('.');
            final String fileid = file.substring(begin + 1, end);
            log.info("handlePlaybackCommand: try load file: {}", fileid);
            L16File l16file = _id2L16File.get(fileid);
            if (l16file == null ) {
                final String fullPath = _l16_path + fileid + ".l16";
                final File l16 = new File(fullPath);
                if (!l16.exists()) {
                    log.warn("handlePlaybackCommand: {} not exist, ignore playback {}", fullPath, fileid);
                    return;
                }
                try (final DataInputStream is = new DataInputStream(new FileInputStream(l16))) {
                    l16file = L16File.loadL16(is);
                    if (l16file == null) {
                        log.warn("handlePlaybackCommand: load {} failed!", fullPath);
                        return;
                    }
                    final L16File prevL16file = _id2L16File.putIfAbsent(fileid, l16file);
                    if (prevL16file != null) {
                        l16file = prevL16file;
                    }
                } catch (IOException ex) {
                    log.warn("handlePlaybackCommand: load {} failed: {}", fullPath, ex.toString());
                    return;
                }
            }
            sendEvent(webSocket, "PlaybackStart",
                    new PayloadPlaybackStart(file,
                            l16file.header.rate,
                            l16file.header.interval,
                            l16file.header.channels));
            schedulePlayback(l16file, file, webSocket);
             */

    private void handlePlaybackCommand(final HubCommandVO cmd, final WebSocket webSocket) {
        final String file = cmd.getPayload().get("file");
        // {vars_playback_id=73cb4b57-0c54-42aa-98a9-9b81352c0cc4}/mnt/aicall/aispeech/dd_app_sb_3_0/c264515130674055869c16fcc2458109.wav
        if (file == null ) {
            log.warn("Playback: {} 's file is null, abort", webSocket.getRemoteSocketAddress());
            return;
        }
        final MediaSession session = webSocket.getAttachment();
        if (session == null) {
            log.error("Playback: {} without Session, abort", webSocket.getRemoteSocketAddress());
            return;
        }
        final int prefixBegin = file.indexOf(_oss_match_prefix);
        if (-1 == prefixBegin) {
            log.warn("handlePlaybackCommand: can't match prefix for {}, ignore playback command!", file);
            return;
        }
        if (!session.startPlaying()) {
            log.error("Playback: {} playing another, abort this play command", webSocket.getRemoteSocketAddress());
            return;
        }
        final String objectName = file.substring(prefixBegin + _oss_match_prefix.length());
        _ossDownloader.submit(() -> {
            try (final OSSObject ossObject = _ossClient.getObject(_oss_bucket, objectName)) {
                final ByteArrayOutputStream os = new ByteArrayOutputStream((int) ossObject.getObjectMetadata().getContentLength());
                ossObject.getObjectContent().transferTo(os);
                final AudioInputStream audioInputStream = AudioSystem.getAudioInputStream(new ByteArrayInputStream(os.toByteArray()));
                final AudioFormat format = audioInputStream.getFormat();

                // interval = 20 ms
                int interval = 20;

                int lenInBytes = (int) (format.getSampleRate() / (1000 / interval) * (format.getSampleSizeInBits() / 8)) * format.getChannels();
                log.info("wav info: sample rate: {}/interval: {}/channels: {}/bytes per interval: {}",
                        format.getSampleRate(), interval, format.getChannels(), lenInBytes);

                sendEvent(webSocket, "PlaybackStart",
                        new PayloadPlaybackStart(file,
                                (int) format.getSampleRate(),
                                interval,
                                format.getChannels()));

                schedulePlayback(audioInputStream, lenInBytes, interval, file, webSocket, session::stopPlaying);
            } catch (IOException | UnsupportedAudioFileException ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    private void schedulePlayback(final InputStream is, final int lenInBytes, final int interval, final String file,
                                  final WebSocket webSocket, final Runnable onEnd) {
        final List<ScheduledFuture<?>> futures = new ArrayList<>();

        long delay = interval;
        int idx = 0;
        try (is) {
            while (true) {
                final byte[] bytes = new byte[lenInBytes];
                final int readSize = is.read(bytes);
                log.info("{}: schedulePlayback read {} bytes", ++idx, readSize);
                if (readSize == lenInBytes) {
                    final ScheduledFuture<?> future = _playbackExecutor.schedule(() -> webSocket.send(bytes), delay, TimeUnit.MILLISECONDS);
                    futures.add(future);
                    delay += interval;
                } else {
                    break;
                }
            }
        } catch (IOException ex) {
            log.warn("schedulePlayback: {}", ex.toString());
        }
        futures.add(_playbackExecutor.schedule(()->{
                    sendEvent(webSocket, "PlaybackStop", new PayloadPlaybackStop(file));
                    onEnd.run();
                },
                delay, TimeUnit.MILLISECONDS));
        log.info("schedulePlayback: schedule playback by {} send action", futures.size());
    }

    private void schedulePlayback(final L16File l16file, final String file, final WebSocket webSocket) {
        final List<ScheduledFuture<?>> futures = new ArrayList<>(l16file.slices.length + 1);
        for (L16File.L16Slice slice : l16file.slices) {
            final ScheduledFuture<?> future = _playbackExecutor.schedule(()->webSocket.send(slice.raw_data),
                    slice.from_start / 1000, TimeUnit.MILLISECONDS);
            futures.add(future);
        }
        final long notifyStopDelay = l16file.slices[l16file.slices.length - 1].from_start / 1000 + l16file.header.interval;
        futures.add(_playbackExecutor.schedule(()->sendEvent(webSocket, "PlaybackStop", new PayloadPlaybackStop(file)),
                notifyStopDelay, TimeUnit.MILLISECONDS));
        log.info("schedulePlayback: schedule {} by {} send action", file, futures.size());
    }

    private static <PAYLOAD> void sendEvent(final WebSocket webSocket, final String eventName, final PAYLOAD payload) {
        final HubEventVO<PAYLOAD> event = new HubEventVO<>();
        final HubEventVO.Header header = new HubEventVO.Header();
        header.setName(eventName);
        event.setHeader(header);
        event.setPayload(payload);
        try {
            webSocket.send(new ObjectMapper().writeValueAsString(event));
        } catch (JsonProcessingException ex) {
            log.warn("sendEvent {}: {}, an error occurred when parseAsJson: {}",
                    webSocket.getRemoteSocketAddress(), event, ex.toString());
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

            speechTranscriber = buildSpeechTranscriber(agent, buildTranscriberListener(webSocket, agent, startConnectingInMs));
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
    private SpeechTranscriberListener buildTranscriberListener(final WebSocket webSocket, final ASRAgent account, final long startConnectingInMs) {
        return new SpeechTranscriberListener() {
            @Override
            public void onTranscriberStart(final SpeechTranscriberResponse response) {
                //task_id是调用方和服务端通信的唯一标识，遇到问题时，需要提供此task_id。
                log.info("onTranscriberStart: task_id={}, name={}, status={}, cost={} ms",
                        response.getTaskId(),
                        response.getName(),
                        response.getStatus(),
                        System.currentTimeMillis() - startConnectingInMs);
                notifyTranscriptionStarted(webSocket, account, response);
            }

            @Override
            public void onSentenceBegin(final SpeechTranscriberResponse response) {
                log.info("onSentenceBegin: task_id={}, name={}, status={}",
                        response.getTaskId(), response.getName(), response.getStatus());
                notifySentenceBegin(webSocket, response);
            }

            @Override
            public void onSentenceEnd(final SpeechTranscriberResponse response) {
                log.info("onSentenceEnd: task_id={}, name={}, status={}, index={}, result={}, confidence={}, begin_time={}, time={}",
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
                log.info("onTranscriptionResultChange: task_id={}, name={}, status={}, index={}, result={}, time={}",
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
                log.info("onTranscriptionComplete: task_id={}, name={}, status={}",
                        response.getTaskId(), response.getName(), response.getStatus());
                notifyTranscriptionCompleted(webSocket, account, response);
            }

            @Override
            public void onFail(final SpeechTranscriberResponse response) {
                log.warn("onFail: task_id={}, status={}, status_text={}",
                        response.getTaskId(),
                        response.getStatus(),
                        response.getStatusText());
            }
        };
    }

    private void notifyTranscriptionStarted(final WebSocket webSocket, final ASRAgent account, final SpeechTranscriberResponse response) {
        final MediaSession session = webSocket.getAttachment();
        session.transcriptionStarted();
        account.incConnected();
        sendEvent(webSocket, "TranscriptionStarted", (Void)null);
    }

    private void notifySentenceBegin(final WebSocket webSocket, final SpeechTranscriberResponse response) {
        sendEvent(webSocket, "SentenceBegin",
                new PayloadSentenceBegin(response.getTransSentenceIndex(), response.getTransSentenceTime()));
    }

    private void notifySentenceEnd(final WebSocket webSocket, final SpeechTranscriberResponse response) {
        sendEvent(webSocket, "SentenceEnd",
                new PayloadSentenceEnd(response.getTransSentenceIndex(),
                        response.getTransSentenceTime(),
                        response.getSentenceBeginTime(),
                        response.getTransSentenceText(),
                        response.getConfidence()));
    }

    private void notifyTranscriptionResultChanged(final WebSocket webSocket, final SpeechTranscriberResponse response) {
        sendEvent(webSocket, "TranscriptionResultChanged",
                new PayloadTranscriptionResultChanged(response.getTransSentenceIndex(),
                        response.getTransSentenceTime(),
                        response.getTransSentenceText()));
    }

    private void notifyTranscriptionCompleted(final WebSocket webSocket, final ASRAgent account, final SpeechTranscriberResponse response) {
        // TODO: account.dec??
        sendEvent(webSocket, "TranscriptionCompleted", (Void)null);
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
        _playbackExecutor.shutdownNow();
        _ossDownloader.shutdownNow();

        log.info("ASR-Hub: shutdown");
    }
}