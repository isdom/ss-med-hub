package com.yulore.metric;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;
import io.prometheus.client.exporter.common.TextFormat;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.StringWriter;

@Slf4j
@Component
public class MetricsPusher {
    private final PushGateway pushGateway;
    private final PrometheusMeterRegistry registry;
    private final String jobName = "default";

    public MetricsPusher(final PushGateway pushGateway, final PrometheusMeterRegistry registry) {
        this.pushGateway = pushGateway;
        this.registry = registry;
    }

    @Scheduled(fixedRate = 30_000)  // 每30秒推送一次
    public void pushMetrics() {
        try {
            final CollectorRegistry promRegistry = registry.getPrometheusRegistry();
            {
                StringWriter writer = new StringWriter();
                TextFormat.write004(writer, promRegistry.metricFamilySamples());
                log.debug("pushMetrics: pushGateway.pushAdd metricFamilySamples \n {}\n with {}", writer, jobName);
            }
            pushGateway.pushAdd(promRegistry, jobName);
            log.debug("pushMetrics: pushGateway.pushAdd ended for {}", jobName);
        } catch (Exception ex) {
            // 处理异常（如重试或日志报警）
            log.warn("pushMetrics: pushGateway.pushAdd failed", ex);
        }
    }
}