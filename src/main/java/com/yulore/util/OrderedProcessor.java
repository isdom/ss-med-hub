package com.yulore.util;

import com.yulore.metric.DisposableGauge;
import com.yulore.metric.MetricCustomized;
import io.micrometer.core.instrument.Counter;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
@Service
public class OrderedProcessor implements OrderedExecutor {
    // 使用连接ID的哈希绑定固定线程
    private static final int POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;

    private final ExecutorService[] workers = new ExecutorService[POOL_SIZE];
    private final Counter[] taskCounters = new Counter[POOL_SIZE];

    private final ObjectProvider<DisposableGauge> gaugeProvider;
    private final ObjectProvider<Counter> counterProvider;

    @PostConstruct
    private void init() {
        for (int i = 0; i < POOL_SIZE; i++) {
            final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(),
                    new DefaultThreadFactory("ASR-Transmit-" + i));
            final BlockingQueue<Runnable> queue = executor.getQueue();
            gaugeProvider.getObject((Supplier<Number>)queue::size, "mh.transmit.qsize",
                    MetricCustomized.builder().tags(List.of("idx", Integer.toString(i))).build());
            workers[i] = executor;
            taskCounters[i] = counterProvider.getObject("mh.transmit.cnt",
                    MetricCustomized.builder().tags(List.of("idx", Integer.toString(i))).build());
            log.info("create ASR-Transmit-{}", i);
        }
    }

    @PreDestroy
    public void release() {
        for (int i = 0; i < POOL_SIZE; i++) {
            workers[i].shutdownNow();
        }
    }

    @Override
    public void submit(final int idx, final Runnable task) {
        int order = idx % POOL_SIZE;
        workers[order].submit(task);
        taskCounters[order].increment();
    }

    @Override
    public int idx2order(final int idx) {
        return idx % POOL_SIZE;
    }
}
