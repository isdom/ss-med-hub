package com.yulore.medhub.service;

import io.micrometer.core.instrument.Gauge;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.*;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
@Service
public class OrderedTaskProcessor implements OrderedTaskExecutor {
    // 使用连接ID的哈希绑定固定线程
    private static final int POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;

    private final ExecutorService[] workers = new ExecutorService[POOL_SIZE];

    private final ObjectProvider<Gauge> gaugeProvider;

    @PostConstruct
    private void init() {
        for (int i = 0; i < POOL_SIZE; i++) {
            final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(),
                    new DefaultThreadFactory("ASR-Transmit-" + i));
            final BlockingQueue<Runnable> queue = executor.getQueue();
            gaugeProvider.getObject((Supplier<Number>)queue::size, "mh.transmit.qsize", "",
                    new String[]{"idx", Integer.toString(i)});
            workers[i] = executor;
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
    public void submit(final int ownerIdx, final Runnable task) {
        workers[ownerIdx % POOL_SIZE].submit(task);
    }
}
