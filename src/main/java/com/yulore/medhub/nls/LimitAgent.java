package com.yulore.medhub.nls;

import io.micrometer.core.instrument.Timer;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RedissonClient;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

@Data
@ToString
@Slf4j
public class LimitAgent<AGENT extends LimitAgent<?>> {
    private final String name;
    private final Timer timer;
    private int limit = 0;

    private final AtomicInteger connectingOrConnectedCount = new AtomicInteger(0);
    private final AtomicInteger connectedCount = new AtomicInteger(0);

    private final RAtomicLong _sharedCounter;

    public static <AGENT extends LimitAgent<?>> CompletionStage<AGENT> attemptSelectAgentAsync(
            final Iterator<AGENT> iterator,
            final CompletableFuture<AGENT> resultFuture,
            // final Timer timer,
            final Executor executor) {
        if (!iterator.hasNext()) {
            final RuntimeException ex = new RuntimeException("All Agents are full");
            if (null != executor) {
                executor.execute(()->resultFuture.completeExceptionally(ex));
            } else {
                resultFuture.completeExceptionally(ex);
            }
            return resultFuture;
        }
        final AGENT agent = iterator.next();
        agent.checkAndSelectIfHasIdleAsync(/*timer*/).whenComplete((selected, ex) -> {
            if (ex != null) {
                log.error("Error selecting agent", ex);
                attemptSelectAgentAsync(iterator, resultFuture, /*timer,*/ executor); // 继续下一个代理
                return;
            }
            if (selected != null) {
                final Runnable doComplete = () -> {
                    log.info("select agent({}): {}/{}", agent.getName(), agent.getConnectingOrConnectedCount().get(), agent.getLimit());
                    resultFuture.complete((AGENT) selected); // 成功选择
                };
                if (null != executor) {
                    executor.execute(doComplete);
                } else {
                    doComplete.run();
                }
            } else {
                attemptSelectAgentAsync(iterator, resultFuture, /*timer,*/ executor); // 当前代理无资源，尝试下一个
            }
        });
        return resultFuture;
    }

    public LimitAgent(final String name, final String sharedTemplate, final RedissonClient redisson, final Timer timer) {
        this.name = name;
        this.timer = timer;
        // 获取分布式计数器
        final String counterKey = String.format(sharedTemplate, this.name);
        log.info("{}: {} => shared counter: {}", this.getClass().getSimpleName(), this.name, counterKey);
        this._sharedCounter = redisson.getAtomicLong(counterKey);
    }

    public CompletionStage<AGENT> checkAndSelectIfHasIdleAsync(/*final Timer timer*/) {
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
