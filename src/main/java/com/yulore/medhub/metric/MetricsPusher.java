package com.yulore.medhub.metric;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class MetricsPusher {
    private final PushGateway pushGateway;
    private final PrometheusMeterRegistry registry;
    private final String jobName = "redisson_async_tasks";

    public MetricsPusher(final PushGateway pushGateway, final PrometheusMeterRegistry registry) {
        this.pushGateway = pushGateway;
        this.registry = registry;
    }

    @Scheduled(fixedRate = 30_000)  // 每30秒推送一次
    public void pushMetrics() {
        try {
            CollectorRegistry promRegistry = registry.getPrometheusRegistry();
            pushGateway.pushAdd(promRegistry, jobName);
        } catch (Exception e) {
            // 处理异常（如重试或日志报警）
        }
    }
}