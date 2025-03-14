package com.yulore.metric;

import com.yulore.util.NetworkUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Arrays;

@Slf4j
public class AsyncTaskMetrics {
    private static final String HOSTNAME = NetworkUtil.getHostname();

    private final Timer asyncTaskTimer;

    public AsyncTaskMetrics(final MeterRegistry registry, final String name, final String desc) {
        // 定义指标名称、标签、分位数
        asyncTaskTimer = Timer.builder(name)
                .description(desc)
                .tags("service", "nls", "hostname", HOSTNAME)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofMillis(1000))
                .register(registry);
        log.info("AsyncTaskMetrics: create {}/{}", name, asyncTaskTimer);
    }

    public AsyncTaskMetrics(final MeterRegistry registry, final String name, final String desc, final String[] tags) {
        // 定义指标名称、标签、分位数
        asyncTaskTimer = Timer.builder(name)
                .description(desc)
                .tags("hostname", HOSTNAME)
                .tags(Tags.of(tags))
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofMillis(1000))
                .register(registry);
        log.info("Timer: create {} with tags:{}", name, Arrays.toString(tags));
    }

    public Timer getTimer() {
        return asyncTaskTimer;
    }
}