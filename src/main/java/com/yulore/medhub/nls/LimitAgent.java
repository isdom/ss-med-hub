package com.yulore.medhub.nls;

import com.alibaba.nls.client.AccessToken;
import com.alibaba.nls.client.protocol.NlsClient;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriber;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriberListener;
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
public class LimitAgent<AGENT extends LimitAgent<?>> {
    private final String name;
    private int limit = 0;

    private final AtomicInteger connectingOrConnectedCount = new AtomicInteger(0);
    private final AtomicInteger connectedCount = new AtomicInteger(0);

    private final RAtomicLong _sharedCounter;

    public LimitAgent(final String name, final String sharedTemplate, final RedissonClient redisson) {
        this.name = name;

        // 获取分布式计数器
        final String counterKey = String.format(sharedTemplate, this.name);
        log.info("{}: {} => shared counter: {}", this.getClass().getSimpleName(), this.name, counterKey);
        this._sharedCounter = redisson.getAtomicLong(counterKey);
    }

    public AGENT checkAndSelectIfHasIdle() {
        while (true) {
            // int currentCount = _connectingOrConnectedCount.get();
            long current = _sharedCounter.get();
            if (current >= limit) {
                // 已经超出限制的并发数
                return null;
            }

            // 原子递增操作
            if (_sharedCounter.compareAndSet(current, current + 1)) {
                // 更新本地计数器用于监控
                connectingOrConnectedCount.set((int) current + 1);
                // 当前的值设置成功，表示 已经成功占用了一个 并发数
                return (AGENT) this;
            }
            // 若未成功占用，表示有别的线程进行了分配，从头开始检查是否还满足可分配的条件
        }
    }

    public int decConnection() {
        // 减少 连接中或已连接的计数
        final long current = _sharedCounter.decrementAndGet();
        // 更新本地计数器用于监控
        connectingOrConnectedCount.decrementAndGet();
        return (int)current;
    }

    public void incConnected() {
        // 增加 已连接的计数
        connectedCount.incrementAndGet();
    }

    public void decConnected() {
        // 减少 已连接的计数
        connectedCount.decrementAndGet();
    }
}
