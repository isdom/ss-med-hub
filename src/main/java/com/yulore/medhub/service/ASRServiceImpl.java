package com.yulore.medhub.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.nls.client.protocol.InputFormatEnum;
import com.alibaba.nls.client.protocol.NlsClient;
import com.alibaba.nls.client.protocol.SampleRateEnum;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriber;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriberListener;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriberResponse;
import com.tencent.asrv2.*;
import com.tencent.core.ws.SpeechClient;
import com.yulore.medhub.metric.AsyncTaskMetrics;
import com.yulore.medhub.nls.ASRAgent;
import com.yulore.medhub.nls.LimitAgent;
import com.yulore.medhub.nls.TxASRAgent;
import com.yulore.medhub.session.*;
import com.yulore.medhub.vo.*;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;
import org.java_websocket.exceptions.WebsocketNotConnectedException;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.*;

@Slf4j
@Service
@ConditionalOnProperty(prefix = "nls", name = "asr-enabled", havingValue = "true")
class ASRServiceImpl implements ASRService {
    @Override
    public void startTranscription(final WSCommandVO cmd, final WebSocket webSocket) {
        final String provider = cmd.getPayload() != null ? cmd.getPayload().get("provider") : null;
        final ASRActor actor = webSocket.getAttachment();
        if (actor == null) {
            log.error("StartTranscription: {} without ASRSession, abort", webSocket.getRemoteSocketAddress());
            return;
        }

        {
            actor.lock();

            if (!actor.startTranscription()) {
                actor.unlock();
                log.warn("StartTranscription: {}'s Session startTranscription already, ignore", webSocket.getRemoteSocketAddress());
                return;
            }

            if ("tx".equals(provider)) {
                startWithTxasr(webSocket, actor, cmd).whenComplete( (v,ex) -> {
                    actor.unlock();
                    if (ex != null) {
                        log.error("StartTranscription: failed: {}", ex.toString());
                        // throw new RuntimeException(ex);
                    }
                });
            } else {
                startWithAliasr(webSocket, actor, cmd).whenComplete( (v,ex) -> {
                    actor.unlock();
                    if (ex != null) {
                        log.error("StartTranscription: failed: {}", ex.toString());
                        // throw new RuntimeException(ex);
                    }
                });
            }
        } /*catch (Exception ex) {*/
            // TODO: close websocket?
//            log.error("StartTranscription: failed: {}", ex.toString());
//            throw new RuntimeException(ex);
    }

    @Override
    public void stopTranscription(final WSCommandVO cmd, final WebSocket webSocket) {
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

    @PostConstruct
    public void start() {
        //创建NlsClient实例应用全局创建一个即可。生命周期可和整个应用保持一致，默认服务地址为阿里云线上服务地址。
        _nlsClient = new NlsClient(_nls_url, "invalid_token");
        _txClient = new SpeechClient(AsrConstant.DEFAULT_RT_REQ_URL);

        initASRAgents(_nlsClient);
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        _nlsClient.shutdown();
        _txClient.shutdown();

        log.info("NlsServiceImpl: shutdown");
    }

    private void initASRAgents(final NlsClient client) {
        _asrAgents.clear();
        if (_all_asr != null) {
            for (Map.Entry<String, String> entry : _all_asr.entrySet()) {
                log.info("asr: {} / {}", entry.getKey(), entry.getValue());
                final String[] values = entry.getValue().split(" ");
                log.info("asr values detail: {}", Arrays.toString(values));
                final ASRAgent agent = ASRAgent.parse(_aliasr_prefix + ":%s", redisson, entry.getKey(), entry.getValue());
                if (null == agent) {
                    log.warn("asr init failed by: {}/{}", entry.getKey(), entry.getValue());
                } else {
                    agent.client = client;
                    _asrAgents.add(agent);
                }
            }
        }
        log.info("asr agent init, count:{}", _asrAgents.size());

        _txasrAgents.clear();
        if (_all_txasr != null) {
            for (Map.Entry<String, String> entry : _all_txasr.entrySet()) {
                log.info("txasr: {} / {}", entry.getKey(), entry.getValue());
                final String[] values = entry.getValue().split(" ");
                log.info("txasr values detail: {}", Arrays.toString(values));
                final TxASRAgent agent = TxASRAgent.parse(_txasr_prefix + ":%s", redisson, entry.getKey(), entry.getValue());
                if (null == agent) {
                    log.warn("txasr init failed by: {}/{}", entry.getKey(), entry.getValue());
                } else {
                    agent.client = _txClient;
                    _txasrAgents.add(agent);
                }
            }
        }
        log.info("txasr agent init, count:{}", _txasrAgents.size());

        schedulerProvider.getObject().scheduleAtFixedRate(this::checkAndUpdateASRToken, 0, 10, TimeUnit.MINUTES);
    }

    public CompletionStage<ASRAgent> selectASRAgentAsync() {
        return LimitAgent.attemptSelectAgentAsync(new ArrayList<>(_asrAgents).iterator(), new CompletableFuture<>(), selectIdleASR.getTimer());
    }

    public CompletionStage<TxASRAgent> selectTxASRAgentAsync() {
        return LimitAgent.attemptSelectAgentAsync(new ArrayList<>(_txasrAgents).iterator(), new CompletableFuture<>(), selectIdleTxASR.getTimer());
    }

    private void checkAndUpdateASRToken() {
        for (final ASRAgent agent : _asrAgents) {
            agent.checkAndUpdateAccessToken();
        }
    }

    private CompletionStage<Void> startWithTxasr(final WebSocket webSocket, final ASRActor session, final WSCommandVO cmd) {
        final long startConnectingInMs = System.currentTimeMillis();
        final io.micrometer.core.instrument.Timer.Sample sample =
                io.micrometer.core.instrument.Timer.start();
        return selectTxASRAgentAsync().whenCompleteAsync((agent, ex) -> {
            sample.stop(selectTxASRAgent.getTimer());
            if (ex != null) {
                log.error("Failed to select TxASR agent", ex);
                webSocket.close();
                return;
            }

            try {
                final SpeechRecognizer speechRecognizer = agent.buildSpeechRecognizer(
                        buildRecognizerListener(session, webSocket, agent, session.sessionId(), startConnectingInMs),
                        (request) -> {
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

                session.setASR(() -> {
                    // int dec_cnt = 0;
                    try {
                        agent.decConnectionAsync().whenComplete((current, ex2) -> {
                            log.info("release txasr({}): {}/{}", agent.getName(), current, agent.getLimit());
                        });
                        try {
                            //通知服务端语音数据发送完毕，等待服务端处理完成。
                            long now = System.currentTimeMillis();
                            log.info("recognizer wait for complete");
                            speechRecognizer.stop();
                            log.info("recognizer stop() latency : {} ms", System.currentTimeMillis() - now);
                        } catch (final Exception ex2) {
                            log.warn("handleStopAsrCommand error", ex2);
                        }

                        speechRecognizer.close();

                        if (session.isTranscriptionStarted()) {
                            // 对于已经标记了 TranscriptionStarted 的会话, 将其使用的 ASR Account 已连接通道减少一
                            agent.decConnected();
                        }
                    } finally {
                    }
                }, (bytes) -> speechRecognizer.write(bytes.array()));

                try {
                    speechRecognizer.start();
                } catch (Exception ex2) {
                    log.error("recognizer.start() error", ex2);
                }
            } catch (Exception ex2) {
                log.error("buildSpeechRecognizer error", ex2);
            }
        }, executorProvider.getObject()).thenAccept(agent->{});
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

    private CompletionStage<Void> startWithAliasr(final WebSocket webSocket, final ASRActor actor, final WSCommandVO cmd) {
        final long startConnectingInMs = System.currentTimeMillis();
        final io.micrometer.core.instrument.Timer.Sample sample =
                io.micrometer.core.instrument.Timer.start();
        return selectASRAgentAsync().whenCompleteAsync((agent, ex) -> {
            sample.stop(selectASRAgent.getTimer());
            if (ex != null) {
                log.error("Failed to select ASR agent", ex);
                webSocket.close();
                return;
            }

            try {
                final SpeechTranscriber transcriber = actor.onSpeechTranscriberCreated(
                        buildSpeechTranscriber(agent, buildTranscriberListener(actor, webSocket, agent, actor.sessionId(), startConnectingInMs)));

                if (cmd.getPayload() != null && cmd.getPayload().get("speech_noise_threshold") != null) {
                    // ref: https://help.aliyun.com/zh/isi/developer-reference/websocket#sectiondiv-rz2-i36-2gv
                    transcriber.addCustomedParam("speech_noise_threshold", Float.parseFloat(cmd.getPayload().get("speech_noise_threshold")));
                }

                actor.setASR(() -> {
                    // int dec_cnt = 0;
                    try {
                        agent.decConnectionAsync().whenComplete((current, ex2) -> {
                            log.info("release asr({}): {}/{}", agent.getName(), current, agent.getLimit());
                        });
                        try {
                            //通知服务端语音数据发送完毕，等待服务端处理完成。
                            long now = System.currentTimeMillis();
                            log.info("transcriber wait for complete");
                            transcriber.stop();
                            log.info("transcriber stop() latency : {} ms", System.currentTimeMillis() - now);
                        } catch (final Exception ex2) {
                            log.warn("handleStopAsrCommand error", ex2);
                        }

                        transcriber.close();

                        if (actor.isTranscriptionStarted()) {
                            // 对于已经标记了 TranscriptionStarted 的会话, 将其使用的 ASR Account 已连接通道减少一
                            agent.decConnected();
                        }
                    } finally {
                    }
                }, (bytes) -> transcriber.send(bytes.array()));

                try {
                    transcriber.start();
                } catch (Exception ex2) {
                    log.error("speechTranscriber.start() error", ex2);
                }
            } catch (Exception ex2) {
                log.error("buildSpeechTranscriber error", ex2);
            }
        }, executorProvider.getObject()).thenAccept(agent->{});
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

    @Qualifier("selectIdleASR")
    @Autowired
    private AsyncTaskMetrics selectIdleASR;

    @Qualifier("selectIdleTxASR")
    @Autowired
    private AsyncTaskMetrics selectIdleTxASR;

    @Qualifier("selectASRAgent")
    @Autowired
    private AsyncTaskMetrics selectASRAgent;

    @Qualifier("selectTxASRAgent")
    @Autowired
    private AsyncTaskMetrics selectTxASRAgent;

    @Value("${nls.url}")
    private String _nls_url;

    @Value("#{${nls.asr}}")
    private Map<String,String> _all_asr;

    @Value("#{${nls.txasr}}")
    private Map<String,String> _all_txasr;

    @Value("${nls.aliasr-prefix}")
    private String _aliasr_prefix;

    @Value("${nls.txasr-prefix}")
    private String _txasr_prefix;

    final List<ASRAgent> _asrAgents = new ArrayList<>();
    final List<TxASRAgent> _txasrAgents = new ArrayList<>();

    @Autowired
    private ObjectProvider<ScheduledExecutorService> schedulerProvider;

    @Autowired
    private RedissonClient redisson;

    @Autowired
    @Qualifier("commonExecutor")
    private ObjectProvider<Executor> executorProvider;

    private NlsClient _nlsClient;

    private SpeechClient _txClient;
}
