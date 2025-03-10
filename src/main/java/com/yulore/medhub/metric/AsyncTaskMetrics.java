package com.yulore.medhub.metric;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

@Slf4j
public class AsyncTaskMetrics {
    private final Timer asyncTaskTimer;

    public AsyncTaskMetrics(final MeterRegistry registry, final String name, final String desc) {
        // 定义指标名称、标签、分位数
        asyncTaskTimer = Timer.builder(name)
                .description(desc)
                .tags("service", "asr-service")  // 自定义标签
                .publishPercentiles(0.75, 0.9, 0.99)  // 75%、90%、99%分位
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofMillis(1000))
                .register(registry);
        log.info("AsyncTaskMetrics: create {}/{}", name, asyncTaskTimer);
    }

    public Timer getTimer() {
        return asyncTaskTimer;
    }
}