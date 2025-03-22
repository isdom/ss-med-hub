package com.yulore.medhub.service;

import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.stereotype.Component;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

@Component
@Slf4j
public class ExecutorRepo implements ApplicationListener<ContextClosedEvent> {
    @Bean(destroyMethod = "shutdown")
    public ScheduledExecutorService scheduledExecutor() {
        log.info("create ScheduledExecutorService");
        return Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors() * 2,
                new DefaultThreadFactory("scheduledExecutor"));
    }

    @Bean
    public Function<String, Executor> buildExecutorProvider() {
        final var builder = buildExecutorServiceProvider();
        return builder::apply;
    }

    @Bean
    public Function<String, ExecutorService> buildExecutorServiceProvider() {
        return name -> {
            final AtomicReference<ExecutorService> created = new AtomicReference<>(null);
            final ExecutorService current = executors.computeIfAbsent(name, k -> {
                created.set(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2,
                        new DefaultThreadFactory(name)));
                return created.get();
            });

            if (created.get() != null) {
                if (created.get() != current) {
                    // mappingFunction invoked & NOT associated with name
                    created.get().shutdownNow();
                } else {
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
}
