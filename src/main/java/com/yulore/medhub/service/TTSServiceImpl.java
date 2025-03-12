package com.yulore.medhub.service;

import com.alibaba.nls.client.protocol.NlsClient;
import com.yulore.medhub.metric.AsyncTaskMetrics;
import com.yulore.medhub.nls.CosyAgent;
import com.yulore.medhub.nls.LimitAgent;
import com.yulore.medhub.nls.TTSAgent;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@Slf4j
@Service
@ConditionalOnProperty(prefix = "nls", name = "tts-enabled", havingValue = "true")
class TTSServiceImpl implements TTSService {
    @PostConstruct
    public void start() {
        //创建NlsClient实例应用全局创建一个即可。生命周期可和整个应用保持一致，默认服务地址为阿里云线上服务地址。
        _nlsClient = new NlsClient(_nls_url, "invalid_token");

        initTTSAgents(_nlsClient);
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        _nlsClient.shutdown();

        log.info("NlsServiceImpl: shutdown");
    }

    private void initTTSAgents(final NlsClient client) {
        if (_all_tts != null) {
            for (Map.Entry<String, String> entry : _all_tts.entrySet()) {
                log.info("tts: {} / {}", entry.getKey(), entry.getValue());
                final String[] values = entry.getValue().split(" ");
                log.info("tts values detail: {}", Arrays.toString(values));
                final TTSAgent agent = TTSAgent.parse(_alitts_prefix + ":%s", redisson, entry.getKey(), entry.getValue());
                if (null == agent) {
                    log.warn("tts init failed by: {}/{}", entry.getKey(), entry.getValue());
                } else {
                    agent.client = client;
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
                final CosyAgent agent = CosyAgent.parse(_alicosy_prefix + ":%s", redisson, entry.getKey(), entry.getValue());
                if (null == agent) {
                    log.warn("cosy init failed by: {}/{}", entry.getKey(), entry.getValue());
                } else {
                    agent.client = client;
                    _cosyAgents.add(agent);
                }
            }
        }
        log.info("cosy agent init, count:{}", _cosyAgents.size());

        schedulerProvider.getObject().scheduleAtFixedRate(this::checkAndUpdateTTSToken, 0, 10, TimeUnit.MINUTES);
    }

    @Override
    public CompletionStage<TTSAgent> selectTTSAgentAsync() {
        return LimitAgent.attemptSelectAgentAsync(new ArrayList<>(_ttsAgents).iterator(), new CompletableFuture<>(),
                selectIdleTTS.getTimer(), executorProvider.getObject());
    }

    @Override
    public CompletionStage<CosyAgent> selectCosyAgentAsync() {
        return LimitAgent.attemptSelectAgentAsync(new ArrayList<>(_cosyAgents).iterator(), new CompletableFuture<>(),
                selectIdleCosy.getTimer(), executorProvider.getObject());
    }

    private void checkAndUpdateTTSToken() {
        for (final TTSAgent agent : _ttsAgents) {
            agent.checkAndUpdateAccessToken();
        }
        for (final CosyAgent agent : _cosyAgents) {
            agent.checkAndUpdateAccessToken();
        }
    }

    @Qualifier("selectIdleTTS")
    @Autowired
    private AsyncTaskMetrics selectIdleTTS;

    @Qualifier("selectIdleCosy")
    @Autowired
    private AsyncTaskMetrics selectIdleCosy;

    @Qualifier("selectTTSAgent")
    @Autowired
    private AsyncTaskMetrics selectTTSAgent;

    @Qualifier("selectCosyAgent")
    @Autowired
    private AsyncTaskMetrics selectCosyAgent;

    @Value("${nls.url}")
    private String _nls_url;

    @Value("#{${nls.tts}}")
    private Map<String,String> _all_tts;

    @Value("#{${nls.cosy}}")
    private Map<String,String> _all_cosy;

    @Value("${nls.alitts-prefix}")
    private String _alitts_prefix;

    @Value("${nls.alicosy-prefix}")
    private String _alicosy_prefix;

    final List<TTSAgent> _ttsAgents = new ArrayList<>();
    final List<CosyAgent> _cosyAgents = new ArrayList<>();

    @Autowired
    private ObjectProvider<ScheduledExecutorService> schedulerProvider;

    @Autowired
    private RedissonClient redisson;

    @Autowired
    @Qualifier("commonExecutor")
    private ObjectProvider<Executor> executorProvider;

    private NlsClient _nlsClient;
}
