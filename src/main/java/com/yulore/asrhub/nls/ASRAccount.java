package com.yulore.asrhub.nls;

import com.alibaba.nls.client.AccessToken;
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
public class ASRAccount {
    String account;
    String appKey;
    String accessKeyId;
    String accessKeySecret;
    int limit = 0;

    AccessToken _accessToken;

    final AtomicInteger _connectingOrConnectedCount = new AtomicInteger(0);
    final AtomicInteger _connectedCount = new AtomicInteger(0);

    final AtomicReference<String> _currentToken = new AtomicReference<String>(null);

    public static ASRAccount parse(final String accountName, final String values) {
        final String[] kvs = values.split(" ");
        final ASRAccount account = new ASRAccount();
        account.setAccount(accountName);

        for (String kv : kvs) {
            final String[] ss = kv.split("=");
            if (ss.length == 2) {
                if (ss[0].equals("appkey")) {
                    account.setAppKey(ss[1]);
                } else if (ss[0].equals("ak_id")) {
                    account.setAccessKeyId(ss[1]);
                } else if (ss[0].equals("ak_secret")) {
                    account.setAccessKeySecret(ss[1]);
                } else if (ss[0].equals("limit")) {
                    account.setLimit(Integer.parseInt(ss[1]));
                }
            }
        }
        if (account.getAppKey() != null && account.getAccessKeyId() != null && account.getAccessKeySecret() != null && account.getLimit() != 0) {
            return account;
        } else {
            return null;
        }
    }

    public String currentToken() {
        return _currentToken.get();
    }

    public ASRAccount checkAndSelectIfhasIdle() {
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

//    public void incConnecting() {
//        // 增加 连接中的计数
//        _connectingOrConnectedCount.incrementAndGet();
//    }

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
                log.info("nls account: {} init token: {}, expire time: {}",
                        account, _accessToken.getToken(),
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
                    log.info("nls account: {} update token: {}, expire time: {}",
                            account, _accessToken.getToken(),
                            new SimpleDateFormat().format(new Date(_accessToken.getExpireTime() * 1000)) );
                } catch (IOException e) {
                    log.warn("_accessToken.apply failed: {}", e.toString());
                }
            } else {
                log.info("nls account: {} no need update token, expire time: {}\nconnecting:{}, connected: {}",
                        account,
                        new SimpleDateFormat().format(new Date(_accessToken.getExpireTime() * 1000)),
                        _connectingOrConnectedCount.get(), _connectedCount.get());
            }
        }
    }
}
