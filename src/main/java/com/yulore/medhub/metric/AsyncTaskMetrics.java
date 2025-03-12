package com.yulore.medhub.metric;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Arrays;

@Slf4j
public class AsyncTaskMetrics {
    private static final String HOSTNAME = getHostnameSafe();

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

    // 安全获取主机名，避免重复调用
    private static String getHostnameSafe() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            log.warn("Failed to get hostname, fallback to 'unknown'", e);
            return "unknown";
        }
    }

    public Timer getTimer() {
        return asyncTaskTimer;
    }
}