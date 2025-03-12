package com.yulore.medhub.service;

import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.concurrent.*;

@Component
@Slf4j
public class ExecutorRepo {
    @Bean(destroyMethod = "shutdown")
    public ScheduledExecutorService scheduledExecutor() {
        log.info("create ScheduledExecutorService");
        return Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors() * 2,
                new DefaultThreadFactory("scheduledExecutor"));
    }

    @Bean(destroyMethod = "shutdown")
    public CommandExecutor commandExecutor() {
        log.info("create CommandExecutor");
        final ExecutorService executor = Executors.newFixedThreadPool(NettyRuntime.availableProcessors() * 2,
                new DefaultThreadFactory("sessionExecutor"));
        return new CommandExecutor() {
            @Override
            public Future<?> submit(final Runnable task) {
                return executor.submit(task);
            }

            public void shutdown() {
                log.info("shutdown CommandExecutor");
                executor.shutdownNow();
            }
        };
    }

    @Bean(name = "commonExecutor", destroyMethod = "shutdown")
    public Executor commonExecutor() {
        log.info("create Common Executor");
        return Executors.newFixedThreadPool(NettyRuntime.availableProcessors() * 2,
                new DefaultThreadFactory("commonExecutor"));
    }
}
