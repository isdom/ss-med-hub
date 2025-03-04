package com.yulore.medhub.service;

import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Component
@Slf4j
public class ExecutorRepo {
    @Bean(destroyMethod = "shutdown")
    public ScheduledExecutorService scheduledExecutor() {
        log.info("create ScheduledExecutorService");
        return Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors() * 2,
                new DefaultThreadFactory("scheduledExecutor"));
    }

    @Bean
    @ConditionalOnProperty(
            prefix = "nlscfg",  // 显式指定前缀
            name = "asrEnabled",  // 使用 kebab-case 格式
            havingValue = "true"
    )
    public ASRService asrService() {
        log.info("create ASRServiceImpl");
        return new ASRServiceImpl();
    }
}
