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
import com.yulore.medhub.nls.ASRAgent;
import com.yulore.medhub.nls.LimitAgent;
import com.yulore.medhub.nls.TxASRAgent;
import com.yulore.medhub.vo.*;
import com.yulore.medhub.vo.cmd.VOStartTranscription;
import com.yulore.medhub.ws.actor.ASRActor;
import com.yulore.metric.MetricCustomized;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;
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
    private Timer asr_started_timer;
    private Timer txasr_started_timer;

    @Override
    public CompletionStage<Timer>  startTranscription(final ASRActor actor, final VOStartTranscription vo, final WebSocket webSocket) {
        if (!actor.startTranscription()) {
            log.warn("StartTranscription: {}'s Session startTranscription already, ignore", webSocket.getRemoteSocketAddress());
            return CompletableFuture.failedStage(new RuntimeException("startTranscription already"));
        }

        if ("tx".equals(vo.provider)) {
            return startWithTxasr(webSocket, actor, vo).whenComplete( (v,ex) -> {
                if (ex != null) {
                    log.warn("StartTranscription with Tx: failed", ex);
                    // throw new RuntimeException(ex);
                }
            }).thenApply(ignored->txasr_started_timer);
        } else {
            return startWithAliasr(webSocket, actor, vo).whenComplete( (v,ex) -> {
                if (ex != null) {
                    log.warn("StartTranscription with Ali: failed", ex);
                    // throw new RuntimeException(ex);
                }
            }).thenApply(ignored -> asr_started_timer);
        }
    }

    @Override
    public void stopTranscription(final WebSocket webSocket) {
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
        asr_started_timer = timerProvider.getObject("nls.asr.started.duration", null);
        txasr_started_timer = timerProvider.getObject("nls.txasr.started.duration", null);
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
                final ASRAgent agent = ASRAgent.parse(
                        _aliasr_prefix + ":%s",
                        redisson,
                        entry.getKey(),
                        entry.getValue());
                if (null == agent) {
                    log.warn("asr init failed by: {}/{}", entry.getKey(), entry.getValue());
                } else {
                    agent.client = client;
                    agent.setSelectIdleTimer(timerProvider.getObject(
                            "nls.asr.idle.select.duration",
                            MetricCustomized.builder().tags(List.of("account", entry.getKey())).build()));
                    agent.setSelectAgentTimer(timerProvider.getObject(
                            "nls.asr.agent.select.duration",
                            MetricCustomized.builder().tags(List.of("account", entry.getKey())).build()));
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
                final TxASRAgent agent = TxASRAgent.parse(
                        _txasr_prefix + ":%s",
                        redisson,
                        entry.getKey(),
                        entry.getValue());
                if (null == agent) {
                    log.warn("txasr init failed by: {}/{}", entry.getKey(), entry.getValue());
                } else {
                    agent.client = _txClient;
                    agent.setSelectIdleTimer(timerProvider.getObject(
                            "nls.txasr.idle.select.duration",
                            MetricCustomized.builder().tags(List.of("account", entry.getKey())).build()));
                    agent.setSelectAgentTimer(timerProvider.getObject(
                            "nls.txasr.agent.select.duration",
                            MetricCustomized.builder().tags(List.of("account", entry.getKey())).build()));
                    _txasrAgents.add(agent);
                }
            }
        }
        log.info("txasr agent init, count:{}", _txasrAgents.size());

        schedulerProvider.getObject().scheduleAtFixedRate(this::checkAndUpdateASRToken, 0, 10, TimeUnit.MINUTES);
    }

    public CompletionStage<ASRAgent> selectASRAgentAsync() {
        final io.micrometer.core.instrument.Timer.Sample sample =
                io.micrometer.core.instrument.Timer.start();
        return LimitAgent.attemptSelectAgentAsync(new ArrayList<>(_asrAgents).iterator(),
                new CompletableFuture<>(),
                executorProvider.getObject()).whenComplete((agent,ex) -> {
                    if (agent != null) {
                        sample.stop(agent.getSelectAgentTimer());
                    }
                });
    }

    public CompletionStage<TxASRAgent> selectTxASRAgentAsync() {
        final io.micrometer.core.instrument.Timer.Sample sample =
                io.micrometer.core.instrument.Timer.start();
        return LimitAgent.attemptSelectAgentAsync(new ArrayList<>(_txasrAgents).iterator(),
                new CompletableFuture<>(),
                executorProvider.getObject()).whenComplete((agent,ex) -> {
                    if (agent != null) {
                        sample.stop(agent.getSelectAgentTimer());
                    }
                });
    }

    private void checkAndUpdateASRToken() {
        for (final ASRAgent agent : _asrAgents) {
            agent.checkAndUpdateAccessToken();
        }
    }

    private CompletionStage<Void> startWithTxasr(final WebSocket webSocket, final ASRActor actor, final VOStartTranscription vo) {
        final long startConnectingInMs = System.currentTimeMillis();
        return selectTxASRAgentAsync().thenCompose(agent -> {
            final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            /*
            if (ex != null) {
                log.error("Failed to select TxASR agent", ex);
                webSocket.close();
                return;
            }
             */

            actor.lock();

            try {
                if (webSocket.getAttachment() != actor) {
                    // detached already
                    agent.decConnectionAsync().whenComplete((current, ex2) -> {
                        log.warn("ws({}) detached, so release asr({}): {}/{}", webSocket.getRemoteSocketAddress(), agent.getName(), current, agent.getLimit());
                    });
                    return completableFuture;
                }

                final SpeechRecognizer speechRecognizer = agent.buildSpeechRecognizer(
                        buildRecognizerListener(actor, completableFuture, agent, actor.sessionId(), startConnectingInMs),
                        (request) -> {
                            // https://cloud.tencent.com/document/product/1093/48982
                            if (vo.noise_threshold != null) {
                                request.setNoiseThreshold(Float.parseFloat(vo.noise_threshold));
                            }
                            if (vo.engine_model_type != null) {
                                request.setEngineModelType(vo.engine_model_type);
                            }
                            if (vo.input_sample_rate != null) {
                                request.setInputSampleRate(Integer.parseInt(vo.input_sample_rate));
                            }
                            if (vo.vad_silence_time != null) {
                                request.setVadSilenceTime(Integer.parseInt(vo.vad_silence_time));
                            }
                        });

                actor.setASR(() -> {
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

                        if (actor.isTranscriptionStarted()) {
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
            } finally {
                actor.unlock();
            }
            return completableFuture;
        });
    }

    private SpeechRecognizerListener buildRecognizerListener(final ASRActor actor,
                                                             final CompletableFuture<Void> completableFuture,
                                                             final TxASRAgent account,
                                                             final String sessionId,
                                                             final long startConnectingInMs) {
        // https://cloud.tencent.com/document/product/1093/48982
        return new SpeechRecognizerListener() {
            @Override
            public void onRecognitionStart(final SpeechRecognizerResponse response) {
                completableFuture.complete(null);
                log.info("onRecognitionStart sessionId={},voice_id:{},{},cost={} ms",
                        sessionId,
                        response.getVoiceId(),
                        JSON.toJSONString(response),
                        System.currentTimeMillis() - startConnectingInMs);
                account.incConnected();
                actor.transcriptionStarted();
            }

            @Override
            public void onSentenceBegin(final SpeechRecognizerResponse response) {
                log.info("onSentenceBegin sessionId={},voice_id:{},{}",
                        sessionId,
                        response.getVoiceId(),
                        JSON.toJSONString(response));
                actor.notifySentenceBegin(new PayloadSentenceBegin(
                        response.getResult().getIndex(), response.getResult().getStartTime().intValue()));
            }

            @Override
            public void onSentenceEnd(final SpeechRecognizerResponse response) {
                log.info("onSentenceEnd sessionId={},voice_id:{},{}",
                        sessionId,
                        response.getVoiceId(),
                        JSON.toJSONString(response));
                actor.notifySentenceEnd(
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
                actor.notifyTranscriptionResultChanged(
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
            }

            @Override
            public void onFail(final SpeechRecognizerResponse response) {
                log.warn("onFail sessionId={},voice_id:{},{}",
                        sessionId,
                        response.getVoiceId(),
                        JSON.toJSONString(response));
                actor.notifySpeechTranscriberFail();
            }

            @Override
            public void onMessage(final SpeechRecognizerResponse response) {
                log.info("{} voice_id:{},{}", "onMessage", response.getVoiceId(), JSON.toJSONString(response));
            }
        };
    }

    private CompletionStage<Void> startWithAliasr(final WebSocket webSocket, final ASRActor actor, final VOStartTranscription vo) {
        final long startConnectingInMs = System.currentTimeMillis();
        return selectASRAgentAsync().thenCompose( agent -> {
            final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            /*
            if (ex != null) {
                log.error("Failed to select ASR agent", ex);
                webSocket.close();
                return;
            }*/

            actor.lock();

            try {
                if (webSocket.getAttachment() != actor) {
                    // detached already
                    agent.decConnectionAsync().whenComplete((current, ex2) -> {
                        log.warn("ws({}) detached, so release asr({}): {}/{}", webSocket.getRemoteSocketAddress(), agent.getName(), current, agent.getLimit());
                    });
                    return completableFuture;
                }

                final SpeechTranscriber transcriber = actor.onSpeechTranscriberCreated(
                        buildSpeechTranscriber(agent, buildTranscriberListener(actor, completableFuture, agent, actor.sessionId(), startConnectingInMs)));

                if (vo.speech_noise_threshold != null) {
                    // ref: https://help.aliyun.com/zh/isi/developer-reference/websocket#sectiondiv-rz2-i36-2gv
                    transcriber.addCustomedParam("speech_noise_threshold", Float.parseFloat(vo.speech_noise_threshold));
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
            } finally {
                actor.unlock();
            }
            return completableFuture;
        });
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

    private SpeechTranscriberListener buildTranscriberListener(final ASRActor actor,
                                                               final CompletableFuture<Void> completableFuture,
                                                               final ASRAgent account,
                                                               final String sessionId,
                                                               final long startConnectingInMs) {
        return new SpeechTranscriberListener() {
            @Override
            public void onTranscriberStart(final SpeechTranscriberResponse response) {
                completableFuture.complete(null);
                //task_id是调用方和服务端通信的唯一标识，遇到问题时，需要提供此task_id。
                log.info("onTranscriberStart: sessionId={}, task_id={}, name={}, status={}, cost={} ms",
                        sessionId,
                        response.getTaskId(),
                        response.getName(),
                        response.getStatus(),
                        System.currentTimeMillis() - startConnectingInMs);
                account.incConnected();
                actor.transcriptionStarted();
            }

            @Override
            public void onSentenceBegin(final SpeechTranscriberResponse response) {
                log.info("onSentenceBegin: sessionId={}, task_id={}, name={}, status={}",
                        sessionId, response.getTaskId(), response.getName(), response.getStatus());
                actor.notifySentenceBegin(new PayloadSentenceBegin(response.getTransSentenceIndex(), response.getTransSentenceTime()));
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
                actor.notifySentenceEnd(
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
                actor.notifyTranscriptionResultChanged(
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
            }

            @Override
            public void onFail(final SpeechTranscriberResponse response) {
                log.warn("onFail: sessionId={}, task_id={}, status={}, status_text={}",
                        sessionId,
                        response.getTaskId(),
                        response.getStatus(),
                        response.getStatusText());
                actor.notifySpeechTranscriberFail();
            }
        };
    }

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

    @Autowired
    private ObjectProvider<Timer> timerProvider;

    private NlsClient _nlsClient;

    private SpeechClient _txClient;
}
