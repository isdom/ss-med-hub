package com.yulore.util;

import com.yulore.metric.DisposableGauge;
import com.yulore.metric.MetricCustomized;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
@Component
public class ExecutorRepo implements ApplicationListener<ContextClosedEvent> {
    @Bean(destroyMethod = "shutdown")
    public ScheduledExecutorService scheduledExecutor() {
        log.info("create ScheduledExecutorService");
        return Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors() * 2,
                new DefaultThreadFactory("scheduledExecutor"));
    }


    @Bean
    public ExecutorStore executorStore() {
        final var builder = buildExecutorServiceProvider();
        return builder::apply;
    }

    @Bean
    public Function<String, ExecutorService> buildExecutorServiceProvider() {
        return name -> {
            final AtomicReference<ThreadPoolExecutor> created = new AtomicReference<>(null);
            final ExecutorService current = executors.computeIfAbsent(name, k -> {
                final int nThreads = Runtime.getRuntime().availableProcessors() * 2;
                created.set(new ThreadPoolExecutor(nThreads, nThreads,
                                0L, TimeUnit.MILLISECONDS,
                                new LinkedBlockingQueue<>(),
                                new DefaultThreadFactory(name)));
                return created.get();
            });

            if (created.get() != null) {
                if (created.get() != current) {
                    // mappingFunction invoked & NOT associated with name
                    created.get().shutdownNow();
                } else {
                    final ThreadPoolExecutor tpe = created.get();
                    final BlockingQueue<Runnable> queue = tpe.getQueue();
                    gaugeProvider.getObject((Supplier<Number>)queue::size, "exc.queue.size",
                            MetricCustomized.builder().tags(List.of("name", name)).build());
                    log.info("create ExecutorService({}) - {}", name, current);
                }
            } else {
                log.info("using exist ExecutorService({}) - {}", name, current);
            }

            return current;
        };
    }

    @Override
    public void onApplicationEvent(final ContextClosedEvent event) {
        // 执行全局销毁动作（如关闭线程池、清理临时文件等）
        log.info("Application is shutting down!");
        while (!executors.isEmpty()) {
            final var first = executors.entrySet().iterator().next();
            try {
                first.getValue().shutdownNow();
            } catch (Exception ignored) {
            }
            executors.remove(first.getKey());
        }
        log.info("Shutdown All Executors");
    }

    private final ConcurrentMap<String, ExecutorService> executors = new ConcurrentHashMap<>();
    private final ObjectProvider<DisposableGauge> gaugeProvider;
}