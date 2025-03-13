package com.yulore.medhub.nls;

import com.tencent.asrv2.SpeechRecognizer;
import com.tencent.asrv2.SpeechRecognizerListener;
import com.tencent.asrv2.SpeechRecognizerRequest;
import com.tencent.core.ws.Credential;
import com.tencent.core.ws.SpeechClient;
import io.micrometer.core.instrument.Timer;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RedissonClient;

import java.util.concurrent.atomic.AtomicReference;
import java.util.UUID;
import java.util.function.Consumer;

@ToString
@Slf4j
public class TxASRAgent extends LimitAgent<TxASRAgent> {
    public SpeechClient client;

    String appKey;
    String accessKeyId;
    String accessKeySecret;

    final AtomicReference<String> _currentToken = new AtomicReference<String>(null);

    public TxASRAgent(final String name, final String sharedTemplate, final RedissonClient redisson) {
        super(name, sharedTemplate, redisson);
    }

    public static TxASRAgent parse(final String sharedTemplate, final RedissonClient redisson, final String accountName, final String values) {
        final String[] kvs = values.split(" ");
        final TxASRAgent agent = new TxASRAgent(accountName, sharedTemplate, redisson);

        for (String kv : kvs) {
            final String[] ss = kv.split("=");
            if (ss.length == 2) {
                switch (ss[0]) {
                    case "appkey" -> agent.appKey = ss[1];
                    case "ak_id" -> agent.accessKeyId = ss[1];
                    case "ak_secret" -> agent.accessKeySecret = ss[1];
                    case "limit" -> agent.setLimit(Integer.parseInt(ss[1]));
                }
            }
        }
        if (agent.appKey != null && agent.accessKeyId != null && agent.accessKeySecret != null && agent.getLimit() > 0) {
            return agent;
        } else {
            return null;
        }
    }

    public SpeechRecognizer buildSpeechRecognizer(final SpeechRecognizerListener listener, final Consumer<SpeechRecognizerRequest> onRequest) throws Exception {
        final Credential credential = new Credential(appKey, accessKeyId, accessKeySecret);
        final SpeechRecognizerRequest request = SpeechRecognizerRequest.init();

        request.setEngineModelType("8k_zh");
        request.setVoiceFormat(1);
        request.setNeedVad(1);

        //voice_id为请求标识，需要保持全局唯一（推荐使用 uuid），遇到问题需要提供该值方便服务端排查
        request.setVoiceId(UUID.randomUUID().toString());

        if (onRequest != null) {
            onRequest.accept(request);
        }

        final SpeechRecognizer recognizer = new SpeechRecognizer(client, credential, request, listener);
        // recognizer.setAppKey(appKey);
        return recognizer;
    }
}
