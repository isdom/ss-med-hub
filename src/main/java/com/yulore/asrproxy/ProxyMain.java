package com.yulore.asrproxy;

import com.alibaba.nls.client.AccessToken;
import com.alibaba.nls.client.protocol.InputFormatEnum;
import com.alibaba.nls.client.protocol.NlsClient;
import com.alibaba.nls.client.protocol.SampleRateEnum;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriber;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriberListener;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriberResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yulore.asrproxy.vo.*;
import io.netty.util.NettyRuntime;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
@Slf4j
@Component
public class ProxyMain {
    //@Value("#{${asr.provider}}")
    //private Map<String,String> _asrProviders;

    @Value("${ws_server.host}")
    private String _ws_host;

    @Value("${ws_server.port}")
    private int _ws_port;

    // in seconds
    //@Value("${ws_server.heartbeat}")
    //private int _ws_heartbeat;

    @Value("${nls.appkey}")
    private String _nls_appkey;

    @Value("${nls.access_key_id}")
    private String _nls_access_key_id;

    @Value("${nls.access_key_secret}")
    private String _nls_access_key_secret;

    @Value("${nls.url}")
    private String _nls_url;

    WebSocketServer _wsServer;

    //final ConcurrentMap<String, WebSocket> _uuid2ws = new ConcurrentHashMap<>();

    private AccessToken _nlsAccessToken;
    private NlsClient _nlsClient;

    private ExecutorService _asyncExecutor;

    @PostConstruct
    public void start() {
        //创建NlsClient实例应用全局创建一个即可。生命周期可和整个应用保持一致，默认服务地址为阿里云线上服务地址。
        _nlsAccessToken = new AccessToken(_nls_access_key_id, _nls_access_key_secret);

        try {
            _nlsAccessToken.apply();
            log.info("get token: , expire time: {}", _nlsAccessToken.getExpireTime());
            _nlsClient = new NlsClient(_nls_url, _nlsAccessToken.getToken());
        } catch (IOException e) {
            log.warn("create Nls Client error: {}", e.toString());
        }

        _asyncExecutor = Executors.newFixedThreadPool(NettyRuntime.availableProcessors() * 2);

        _wsServer = new WebSocketServer(new InetSocketAddress(_ws_host, _ws_port)) {
                    @Override
                    public void onOpen(WebSocket webSocket, ClientHandshake clientHandshake) {
                        log.info("new connection to {}", webSocket.getRemoteSocketAddress());
                    }

                    @Override
                    public void onClose(WebSocket webSocket, int code, String reason, boolean remote) {
                        log.info("closed {} with exit code {} additional info: {}", webSocket.getRemoteSocketAddress(), code, reason);
                        stopAndCloseTranscriber(webSocket);
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
                        log.info("received binary message from {}: isDirect: {}", webSocket.getRemoteSocketAddress(), bytes.isDirect());
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

    private void handleASRData(final ByteBuffer bytes, final WebSocket webSocket) {
        final SpeechTranscriber transcriber = webSocket.getAttachment();
        if (transcriber != null) {
            transcriber.send(bytes.array());
        }
    }

    private void handleASRCommand(final ASRCommandVO cmd, final WebSocket webSocket) {
        if ("StartTranscription".equals(cmd.getHeader().get("name"))) {
            _asyncExecutor.submit(()->handleStartAsrCommand(cmd, webSocket));
        } else if ("StopTranscription".equals(cmd.getHeader().get("name"))) {
            _asyncExecutor.submit(()->handleStopAsrCommand(cmd, webSocket));
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

    private void handleStartAsrCommand(final ASRCommandVO cmd, final WebSocket webSocket) {
        // final String uuid = cmd.getHeader().get("uuid");

        final SpeechTranscriber speechTranscriber = createSpeechTranscriber(_nlsClient, new SpeechTranscriberListener() {
            @Override
            public void onTranscriberStart(final SpeechTranscriberResponse response) {
                //task_id是调用方和服务端通信的唯一标识，遇到问题时，需要提供此task_id。
                log.info("onTranscriberStart: task_id={}, name={}, status={}",
                        response.getTaskId(),
                        response.getName(),
                        response.getStatus());
                notifyTranscriptionStarted(webSocket, response);
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
                notifyTranscriptionCompleted(webSocket, response);
            }

            @Override
            public void onFail(final SpeechTranscriberResponse response) {
                log.warn("onFail: task_id={}, status={}, status_text={}",
                        response.getTaskId(),
                        response.getStatus(),
                        response.getStatusText());
            }
        });

        // _uuid2ws.put(uuid, webSocket);
        try {
            speechTranscriber.start();
        } catch (Exception e) {
            log.error("speechTranscriber.start() error: {}", e.toString());
        }
        webSocket.setAttachment(speechTranscriber);
    }

    private void notifyTranscriptionStarted(final WebSocket webSocket, final SpeechTranscriberResponse response) {
        sendEvent(webSocket, "TranscriptionStarted", new PayloadTranscriptionCompleted());
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

    private void notifyTranscriptionCompleted(final WebSocket webSocket, final SpeechTranscriberResponse response) {
        sendEvent(webSocket, "TranscriptionCompleted", new PayloadTranscriptionCompleted());
    }

    private SpeechTranscriber createSpeechTranscriber(final NlsClient client, final SpeechTranscriberListener listener) {
        SpeechTranscriber transcriber = null;
        try {
            //创建实例、建立连接。
            transcriber = new SpeechTranscriber(client, listener);
            transcriber.setAppKey(_nls_appkey);
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

    private void handleStopAsrCommand(final ASRCommandVO cmd, final WebSocket webSocket) {
        stopAndCloseTranscriber(webSocket);
        webSocket.close();
    }

    private static void stopAndCloseTranscriber(WebSocket webSocket) {
        final SpeechTranscriber transcriber = webSocket.getAttachment();
        if (transcriber != null) {
            webSocket.setAttachment(null);
            try {
                //通知服务端语音数据发送完毕，等待服务端处理完成。
                long now = System.currentTimeMillis();
                log.info("ASR wait for complete");
                transcriber.stop();
                log.info("ASR latency : {} ms", System.currentTimeMillis() - now);
            } catch (Exception e) {
                log.warn("handleStopAsrCommand error: {}", e.toString());
            }

            transcriber.close();
        }
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        _wsServer.stop();
        _nlsClient.shutdown();
        log.info("ASR-Proxy: shutdown");
    }
}