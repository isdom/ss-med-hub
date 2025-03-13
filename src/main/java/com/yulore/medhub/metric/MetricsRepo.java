package com.yulore.medhub.metric;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import static org.springframework.beans.factory.config.ConfigurableBeanFactory.SCOPE_PROTOTYPE;

@RequiredArgsConstructor
@Component
public class MetricsRepo {
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Timer build(final String name, final String desc, final String[] tags) {
        return new AsyncTaskMetrics(meterRegistry, name, desc, tags).getTimer();
    }

    final MeterRegistry meterRegistry;
}
