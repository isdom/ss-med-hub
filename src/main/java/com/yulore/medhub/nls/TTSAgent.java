package com.yulore.medhub.nls;

import com.alibaba.nls.client.AccessToken;
import com.alibaba.nls.client.protocol.NlsClient;
import com.alibaba.nls.client.protocol.tts.SpeechSynthesizer;
import com.alibaba.nls.client.protocol.tts.SpeechSynthesizerListener;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RedissonClient;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Data
@ToString
@Slf4j
public class TTSAgent {
    NlsClient client;

    final String name;
    String appKey;
    String accessKeyId;
    String accessKeySecret;
    int limit = 0;

    AccessToken _accessToken;

    final AtomicInteger _connectingOrConnectedCount = new AtomicInteger(0);
    final AtomicInteger _connectedCount = new AtomicInteger(0);

    final AtomicReference<String> _currentToken = new AtomicReference<String>(null);

    final RAtomicLong _sharedCounter;

    // 新增 Redisson 客户端引用
    private final RedissonClient redisson;

    public TTSAgent(final String name, final String sharedTemplate, final RedissonClient redisson) {
        this.name = name;
        this.redisson = redisson;

        // 获取分布式计数器
        final String counterKey = String.format(sharedTemplate, this.name);
        log.info("TTSAgent: {} => shared counter: {}", this.name, counterKey);
        this._sharedCounter = redisson.getAtomicLong(counterKey);
    }

    public static TTSAgent parse(final String sharedTemplate, final RedissonClient redisson, final String accountName, final String values) {
        final String[] kvs = values.split(" ");
        final TTSAgent agent = new TTSAgent(accountName, sharedTemplate, redisson);

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

    public SpeechSynthesizer buildSpeechSynthesizer(final SpeechSynthesizerListener listener) throws Exception {
        //创建实例、建立连接。
        final SpeechSynthesizer synthesizer = new SpeechSynthesizer(client, currentToken(), listener);
        synthesizer.setAppKey(appKey);
        return synthesizer;
    }

    public String currentToken() {
        return _currentToken.get();
    }

    public TTSAgent checkAndSelectIfHasIdle() {
        while (true) {
            // int currentCount = _connectingOrConnectedCount.get();
            long current = _sharedCounter.get();
            if (current >= limit) {
                // 已经超出限制的并发数
                return null;
            }

            //if (_connectingOrConnectedCount.compareAndSet(currentCount, currentCount + 1)) {
            // 原子递增操作
            if (_sharedCounter.compareAndSet(current, current + 1)) {
                // 更新本地计数器用于监控
                _connectingOrConnectedCount.set((int) current + 1);
                // 当前的值设置成功，表示 已经成功占用了一个 并发数
                return this;
            }
            // 若未成功占用，表示有别的线程进行了分配，从头开始检查是否还满足可分配的条件
        }
    }

    public int decConnection() {
        // 减少 连接中或已连接的计数
        final long current = _sharedCounter.decrementAndGet();
        // 更新本地计数器用于监控
        _connectingOrConnectedCount.decrementAndGet();
        return (int)current;
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
                log.info("tts agent: {} init token: {}, expire time: {}",
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
                    log.info("tts agent: {} update token: {}, expire time: {}",
                            name, _accessToken.getToken(),
                            new SimpleDateFormat().format(new Date(_accessToken.getExpireTime() * 1000)) );
                } catch (IOException e) {
                    log.warn("_accessToken.apply failed: {}", e.toString());
                }
            } else {
                log.info("tts agent: {} no need update token, expire time: {} connecting:{}, connected: {}",
                        name,
                        new SimpleDateFormat().format(new Date(_accessToken.getExpireTime() * 1000)),
                        _connectingOrConnectedCount.get(), _connectedCount.get());
            }
        }
    }
}
