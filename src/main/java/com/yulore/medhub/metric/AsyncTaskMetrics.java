package com.yulore.medhub.metric;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

@Component
public class AsyncTaskMetrics {
    private final Timer asyncTaskTimer;

    public AsyncTaskMetrics(MeterRegistry registry) {
        // 定义指标名称、标签、分位数
        asyncTaskTimer = Timer.builder("redisson.async.task.duration")
                .description("Redisson异步任务执行耗时分布")
                .tags("service", "rpc-service")  // 自定义标签
                .publishPercentiles(0.75, 0.9, 0.99)  // 75%、90%、99%分位
                .register(registry);
    }

    public Timer getTimer() {
        return asyncTaskTimer;
    }
}