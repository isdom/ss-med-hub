package com.yulore.metric;

import com.yulore.util.NetworkUtil;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
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
                .tags(Tags.of(tags))
                .register(meterRegistry);
    }

    final MeterRegistry meterRegistry;
}
