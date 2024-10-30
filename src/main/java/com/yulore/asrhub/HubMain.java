package com.yulore.asrhub;

import com.alibaba.nls.client.protocol.InputFormatEnum;
import com.alibaba.nls.client.protocol.NlsClient;
import com.alibaba.nls.client.protocol.SampleRateEnum;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriber;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriberListener;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriberResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yulore.asrhub.nls.ASRAccount;
import com.yulore.asrhub.session.Session;
import com.yulore.asrhub.vo.*;
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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

    @Value("#{${nls.accounts}}")
    private Map<String,String> _nlsAccounts;

    @Value("${test.enable_delay}")
    private boolean _test_enable_delay;

    @Value("${test.delay_ms}")
    private long _test_delay_ms;

    final List<ASRAccount> _accounts = new ArrayList<>();

    private ScheduledExecutorService _nlsAuthExecutor;
    
    WebSocketServer _wsServer;

    private NlsClient _nlsClient;

    private ExecutorService _sessionExecutor;

    @PostConstruct
    public void start() {
        initASRAccounts();

        //创建NlsClient实例应用全局创建一个即可。生命周期可和整个应用保持一致，默认服务地址为阿里云线上服务地址。
        _nlsClient = new NlsClient(_nls_url, "invalid_token");

        _sessionExecutor = Executors.newFixedThreadPool(NettyRuntime.availableProcessors() * 2);

        _wsServer = new WebSocketServer(new InetSocketAddress(_ws_host, _ws_port)) {
                    @Override
                    public void onOpen(final WebSocket webSocket, final ClientHandshake clientHandshake) {
                        log.info("new connection to {}", webSocket.getRemoteSocketAddress());
                        // init session attach with webSocket
                        webSocket.setAttachment(new Session(_test_enable_delay, _test_delay_ms));
                    }

                    @Override
                    public void onClose(WebSocket webSocket, int code, String reason, boolean remote) {
                        log.info("closed {} with exit code {} additional info: {}", webSocket.getRemoteSocketAddress(), code, reason);
                        stopAndCloseTranscriber(webSocket);
                        webSocket.<Session>getAttachment().close();
                    }

                    @Override
                    public void onMessage(final WebSocket webSocket, final String message) {
                        log.info("received text message from {}: {}", webSocket.getRemoteSocketAddress(), message);
                        try {
                            handleASRCommand(new ObjectMapper().readValue(message, ASRCommandVO.class), webSocket);
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

    private void initASRAccounts() {
        _accounts.clear();
        for (Map.Entry<String, String> entry : _nlsAccounts.entrySet()) {
            log.info("nls account: {} / {}", entry.getKey(), entry.getValue());
            final String[] values = entry.getValue().split(" ");
            log.info("nls values detail: {}", Arrays.toString(values));
            final ASRAccount account = ASRAccount.parse(entry.getKey(), entry.getValue());
            if (null == account) {
                log.warn("nls account init failed by: {}/{}", entry.getKey(), entry.getValue());
            } else {
                _accounts.add(account);
            }
        }
        log.info("nls account init, count:{}", _accounts.size());

        _nlsAuthExecutor = Executors.newSingleThreadScheduledExecutor();
        _nlsAuthExecutor.scheduleAtFixedRate(this::checkAndUpdateNlsToken, 0, 10, TimeUnit.SECONDS);
    }

    private ASRAccount selectASRAccount() {
        for (ASRAccount account : _accounts) {
            final ASRAccount selected = account.checkAndSelectIfhasIdle();
            if (null != selected) {
                log.info("select asr({}): {}/{}", account.getAccount(), account.get_connectingOrConnectedCount().get(), account.getLimit());
                return selected;
            }
        }
        throw new RuntimeException("all asr account has full");
    }

    private void checkAndUpdateNlsToken() {
        for (ASRAccount account : _accounts) {
            account.checkAndUpdateAccessToken();
        }
    }

    private void handleASRData(final ByteBuffer bytes, final WebSocket webSocket) {
        final Session session = webSocket.getAttachment();
        if (session == null) {
            log.error("handleASRData: {} without Session, abort", webSocket.getRemoteSocketAddress());
            return;
        }
        session.transmit(bytes);
    }

    private void handleASRCommand(final ASRCommandVO cmd, final WebSocket webSocket) {
        if ("StartTranscription".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleStartTranscriptionCommand(cmd, webSocket));
        } else if ("StopTranscription".equals(cmd.getHeader().get("name"))) {
            _sessionExecutor.submit(()-> handleStopTranscriptionCommand(cmd, webSocket));
        } else {
            log.warn("handleASRCommand: Unknown Command: {}", cmd);
        }
    }

    private static <PAYLOAD> void sendEvent(final WebSocket webSocket, final String eventName, final PAYLOAD payload) {
        final ASREventVO<PAYLOAD> event = new ASREventVO<>();
        final ASREventVO.Header header = new ASREventVO.Header();
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

    private void handleStartTranscriptionCommand(final ASRCommandVO cmd, final WebSocket webSocket) {
        final Session session = webSocket.getAttachment();
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
            final ASRAccount account = selectASRAccount();
            session.setAsrAccount(account);

            speechTranscriber = buildSpeechTranscriber(_nlsClient, account, buildTranscriberListener(webSocket, account, startConnectingInMs));
            session.setSpeechTranscriber(speechTranscriber);
        } catch (Exception ex) {
            // TODO: close websocket?
            log.error("StartTranscription: failed: {}", ex.toString());
            throw ex;
        } finally {
            session.unlock();
        }

        try {
            speechTranscriber.start();
        } catch (Exception e) {
            log.error("speechTranscriber.start() error: {}", e.toString());
        }
    }

    private SpeechTranscriber buildSpeechTranscriber(final NlsClient client, final ASRAccount account, final SpeechTranscriberListener listener) {
        SpeechTranscriber transcriber = null;
        try {
            log.info("nls_url: {}", _nls_url);

            //创建实例、建立连接。
            transcriber = new SpeechTranscriber(client, account.currentToken(), listener);
            transcriber.setAppKey(account.getAppKey());

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

        } catch (Exception e) {
            log.error("createSpeechTranscriber error: {}", e.toString());
        }
        return transcriber;
    }

    @NotNull
    private SpeechTranscriberListener buildTranscriberListener(final WebSocket webSocket, final ASRAccount account, final long startConnectingInMs) {
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

    private void notifyTranscriptionStarted(final WebSocket webSocket, final ASRAccount account, final SpeechTranscriberResponse response) {
        final Session session = webSocket.getAttachment();
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

    private void notifyTranscriptionCompleted(final WebSocket webSocket, final ASRAccount account, final SpeechTranscriberResponse response) {
        // TODO: account.dec??
        sendEvent(webSocket, "TranscriptionCompleted", (Void)null);
    }

    private void handleStopTranscriptionCommand(final ASRCommandVO cmd, final WebSocket webSocket) {
        stopAndCloseTranscriber(webSocket);
        webSocket.close();
    }

    private static void stopAndCloseTranscriber(final WebSocket webSocket) {
        final Session session = webSocket.getAttachment();
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

        log.info("ASR-Hub: shutdown");
    }
}