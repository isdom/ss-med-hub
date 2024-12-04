package com.yulore.medhub.nls;

import com.tencent.asrv2.SpeechRecognizer;
import com.tencent.asrv2.SpeechRecognizerListener;
import com.tencent.asrv2.SpeechRecognizerRequest;
import com.tencent.core.ws.Credential;
import com.tencent.core.ws.SpeechClient;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.UUID;

@Data
@ToString
@Slf4j
public class TxASRAgent {
    SpeechClient client;

    String name;
    String appKey;
    String accessKeyId;
    String accessKeySecret;
    int limit = 0;

    final AtomicInteger _connectingOrConnectedCount = new AtomicInteger(0);
    final AtomicInteger _connectedCount = new AtomicInteger(0);

    final AtomicReference<String> _currentToken = new AtomicReference<String>(null);

    public static TxASRAgent parse(final String accountName, final String values) {
        final String[] kvs = values.split(" ");
        final TxASRAgent agent = new TxASRAgent();
        agent.setName(accountName);

        for (String kv : kvs) {
            final String[] ss = kv.split("=");
            if (ss.length == 2) {
                switch (ss[0]) {
                    case "appkey" -> agent.setAppKey(ss[1]);
                    case "ak_id" -> agent.setAccessKeyId(ss[1]);
                    case "ak_secret" -> agent.setAccessKeySecret(ss[1]);
                    case "limit" -> agent.setLimit(Integer.parseInt(ss[1]));
                }
            }
        }
        if (agent.getAppKey() != null && agent.getAccessKeyId() != null && agent.getAccessKeySecret() != null && agent.getLimit() != 0) {
            return agent;
        } else {
            return null;
        }
    }

    public SpeechRecognizer buildSpeechRecognizer(final SpeechRecognizerListener listener) throws Exception {
        final Credential credential = new Credential(appKey, accessKeyId, accessKeySecret);
        final SpeechRecognizerRequest request = SpeechRecognizerRequest.init();
        request.setEngineModelType("8k_zh");
        request.setVoiceFormat(1);
        //voice_id为请求标识，需要保持全局唯一（推荐使用 uuid），遇到问题需要提供该值方便服务端排查
        request.setVoiceId(UUID.randomUUID().toString());

        final SpeechRecognizer recognizer = new SpeechRecognizer(client, credential, request, listener);
        // recognizer.setAppKey(appKey);
        return recognizer;
    }

    public TxASRAgent checkAndSelectIfHasIdle() {
        while (true) {
            int currentCount = _connectingOrConnectedCount.get();
            if (currentCount >= limit) {
                // 已经超出限制的并发数
                return null;
            }
            if (_connectingOrConnectedCount.compareAndSet(currentCount, currentCount + 1)) {
                // 当前的值设置成功，表示 已经成功占用了一个 并发数
                return this;
            }
            // 若未成功占用，表示有别的线程进行了分配，从头开始检查是否还满足可分配的条件
        }
    }

    public void decConnection() {
        // 减少 连接中或已连接的计数
        _connectingOrConnectedCount.decrementAndGet();
    }

    public void incConnected() {
        // 增加 已连接的计数
        _connectedCount.incrementAndGet();
    }

    public void decConnected() {
        // 减少 已连接的计数
        _connectedCount.decrementAndGet();
    }
}
