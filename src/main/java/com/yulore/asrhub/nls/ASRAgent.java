package com.yulore.asrhub.nls;

import com.alibaba.nls.client.AccessToken;
import com.alibaba.nls.client.protocol.NlsClient;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriber;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriberListener;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Data
@ToString
@Slf4j
public class ASRAgent {
    NlsClient client;

    String name;
    String appKey;
    String accessKeyId;
    String accessKeySecret;
    int limit = 0;

    AccessToken _accessToken;

    final AtomicInteger _connectingOrConnectedCount = new AtomicInteger(0);
    final AtomicInteger _connectedCount = new AtomicInteger(0);

    final AtomicReference<String> _currentToken = new AtomicReference<String>(null);

    public static ASRAgent parse(final String accountName, final String values) {
        final String[] kvs = values.split(" ");
        final ASRAgent agent = new ASRAgent();
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

    public SpeechTranscriber buildSpeechTranscriber(final SpeechTranscriberListener listener) throws Exception {
        //创建实例、建立连接。
        final SpeechTranscriber transcriber = new SpeechTranscriber(client, currentToken(), listener);
        transcriber.setAppKey(appKey);
        return transcriber;
    }

    public String currentToken() {
        return _currentToken.get();
    }

    public ASRAgent checkAndSelectIfhasIdle() {
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

    public void checkAndUpdateAccessToken() {
        if (_accessToken == null) {
            _accessToken = new AccessToken(accessKeyId, accessKeySecret);
            try {
                _accessToken.apply();
                _currentToken.set(_accessToken.getToken());
                log.info("asr agent: {} init token: {}, expire time: {}",
                        name, _accessToken.getToken(),
                        new SimpleDateFormat().format(new Date(_accessToken.getExpireTime() * 1000)) );

            } catch (IOException e) {
                log.warn("_accessToken.apply failed: {}", e.toString());
            }
        } else {
            if (System.currentTimeMillis() / 1000 + 5 * 60 >= _accessToken.getExpireTime()) {
                // 比到期时间提前 5分钟 进行 AccessToken 的更新
                try {
                    _accessToken.apply();
                    _currentToken.set(_accessToken.getToken());
                    log.info("asr agent: {} update token: {}, expire time: {}",
                            name, _accessToken.getToken(),
                            new SimpleDateFormat().format(new Date(_accessToken.getExpireTime() * 1000)) );
                } catch (IOException e) {
                    log.warn("_accessToken.apply failed: {}", e.toString());
                }
            } else {
                log.info("asr agent: {} no need update token, expire time: {} connecting:{}, connected: {}",
                        name,
                        new SimpleDateFormat().format(new Date(_accessToken.getExpireTime() * 1000)),
                        _connectingOrConnectedCount.get(), _connectedCount.get());
            }
        }
    }
}
