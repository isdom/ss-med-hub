package com.yulore.medhub.nls;

import com.yulore.medhub.metric.AsyncTaskMetrics;
import io.micrometer.core.instrument.Timer;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RFuture;
import org.redisson.api.RedissonClient;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

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

    public CompletionStage<AGENT> checkAndSelectIfHasIdleAsync(final Timer timer) {
        final Timer.Sample sample = Timer.start();
        return attemptSelectAsync(new CompletableFuture<>()).whenComplete((agent, ex)->{
            sample.stop(timer);
        });
    }

    private CompletionStage<AGENT> attemptSelectAsync(final CompletableFuture<AGENT> resultFuture) {
        // 异步获取当前计数器值
        _sharedCounter.getAsync().whenComplete((current, ex) -> {
            if (ex != null) {
                log.warn("attemptSelectAsync: Failed to get current value", ex);
                attemptSelectAsync(resultFuture); // 重试
                return;
            }
            if (current >= limit) {
                resultFuture.complete(null); // 资源已耗尽
                return;
            }
            // 异步尝试 CAS 操作
            _sharedCounter.compareAndSetAsync(current, current + 1)
                    .whenComplete((success, casEx) -> {
                        if (casEx != null) {
                            log.warn("attemptSelectAsync: CAS operation failed", casEx);
                            attemptSelectAsync(resultFuture); // 重试
                            return;
                        }
                        if (success) {
                            connectingOrConnectedCount.set(current.intValue() + 1);
                            resultFuture.complete((AGENT)this); // 选择成功
                        } else {
                            attemptSelectAsync(resultFuture); // CAS 失败，继续重试
                        }
                    });
        });
        return resultFuture;
    }

    public int decConnection() {
        // 减少 连接中或已连接的计数
        final long current = _sharedCounter.decrementAndGet();
        // 更新本地计数器用于监控
        connectingOrConnectedCount.decrementAndGet();
        return (int)current;
    }

    public CompletionStage<Long> decConnectionAsync() {
        return _sharedCounter.decrementAndGetAsync()
                .whenComplete((current, ex) -> {
                    if (ex == null) {
                        connectingOrConnectedCount.decrementAndGet();
                    }
                });
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
