package com.yulore.metric;

import com.yulore.util.NetworkUtil;
import io.micrometer.core.instrument.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.function.Supplier;

import static org.springframework.beans.factory.config.ConfigurableBeanFactory.SCOPE_PROTOTYPE;

@Slf4j
@RequiredArgsConstructor
@Component
public class MetricsRepo {
    private static final String HOSTNAME = NetworkUtil.getHostname();
    private static final String LOCAL_IP = NetworkUtil.getLocalIpv4AsString();

    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Timer buildTimer(final String name, final String desc, final String[] tags) {
        return new AsyncTaskMetrics(meterRegistry, name, desc, tags).getTimer();
    }

    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Gauge buildGauge(final Supplier<Number> f, final String name, final String desc, final String[] tags) {
        log.info("Gauge: create {} with f:{}/tags:{}", name, f, Arrays.toString(tags));
        return Gauge.builder(name, f)
                .description(desc)
                .tags("hostname", HOSTNAME)
                .tags("ip", LOCAL_IP)
                .tags("ns", System.getenv("NACOS_NAMESPACE"))
                .tags("srv", System.getenv("NACOS_DATAID"))
                .tags(Tags.of(tags))
                .register(meterRegistry);
    }

    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Counter buildCounter(final String name, final String desc, final String[] tags) {
        log.info("Counter: create {} with tags:{}", name, Arrays.toString(tags));
        return Counter.builder(name)
                .description(desc)
                .tags("hostname", HOSTNAME)
                .tags("ip", LOCAL_IP)
                .tags("ns", System.getenv("NACOS_NAMESPACE"))
                .tags("srv", System.getenv("NACOS_DATAID"))
                .tags(Tags.of(tags))
                .register(meterRegistry);
    }

    final MeterRegistry meterRegistry;
}
