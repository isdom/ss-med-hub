package com.yulore.medhub.metric;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
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
            final CollectorRegistry promRegistry = registry.getPrometheusRegistry();
            pushGateway.pushAdd(promRegistry, jobName);
            log.info("pushMetrics: pushGateway.pushAdd with {}/{}", promRegistry, jobName);
        } catch (Exception ex) {
            // 处理异常（如重试或日志报警）
            log.warn("pushMetrics: pushGateway.pushAdd failed", ex);
        }
    }
}