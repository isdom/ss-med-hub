package com.yulore.medhub.nls;

import com.alibaba.nls.client.AccessToken;
import com.alibaba.nls.client.protocol.NlsClient;
import com.alibaba.nls.client.protocol.tts.StreamInputTts;
import com.alibaba.nls.client.protocol.tts.StreamInputTtsListener;
import io.micrometer.core.instrument.Timer;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RedissonClient;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

@ToString
@Slf4j
public class CosyAgent extends LimitAgent<CosyAgent> {
    public NlsClient client;

    String appKey;
    String accessKeyId;
    String accessKeySecret;

    AccessToken _accessToken;

    final AtomicReference<String> _currentToken = new AtomicReference<String>(null);

    public CosyAgent(final String name, final String sharedTemplate, final RedissonClient redisson, final Timer timer) {
        super(name, sharedTemplate, redisson, timer);
    }

    public static CosyAgent parse(final String sharedTemplate, final RedissonClient redisson, final String accountName, final Timer timer, final String values) {
        final String[] kvs = values.split(" ");
        final CosyAgent agent = new CosyAgent(accountName, sharedTemplate, redisson, timer);

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

    public String currentToken() {
        return _currentToken.get();
    }

    public void checkAndUpdateAccessToken() {
        if (_accessToken == null) {
            _accessToken = new AccessToken(accessKeyId, accessKeySecret);
            try {
                _accessToken.apply();
                _currentToken.set(_accessToken.getToken());
                log.info("cosy agent: {} init token: {}, expire time: {}",
                        getName(), _accessToken.getToken(),
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
                    log.info("cosy agent: {} update token: {}, expire time: {}",
                            getName(), _accessToken.getToken(),
                            new SimpleDateFormat().format(new Date(_accessToken.getExpireTime() * 1000)) );
                } catch (IOException e) {
                    log.warn("_accessToken.apply failed: {}", e.toString());
                }
            } else {
                log.info("cosy agent: {} no need update token, expire time: {} connecting:{}, connected: {}",
                        getName(),
                        new SimpleDateFormat().format(new Date(_accessToken.getExpireTime() * 1000)),
                        getConnectingOrConnectedCount().get(), getConnectedCount().get());
            }
        }
    }

    public StreamInputTts buildCosyvoiceSynthesizer(final StreamInputTtsListener listener) throws Exception {
        //创建实例、建立连接。
        final StreamInputTts synthesizer = new StreamInputTts(client, currentToken(), listener);
        synthesizer.setAppKey(appKey);
        return synthesizer;
    }

    @Override
    public CompletionStage<Long> decConnectionAsync() {
        return super.decConnectionAsync().whenComplete((current, ex)->{
            log.info("release cosy({}): {}/{}", getName(), current, getLimit());
        });
    }
}
