package com.yulore.metric;

import com.yulore.util.NetworkUtil;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.function.Supplier;

import static org.springframework.beans.factory.config.ConfigurableBeanFactory.SCOPE_PROTOTYPE;

@Slf4j
@RequiredArgsConstructor
@Component
public class MetricsRepo {
    private static final String HOSTNAME = NetworkUtil.getHostname();
    private static final String LOCAL_IP = NetworkUtil.getLocalIpv4AsString();
    public static final String[] EMPTY_STRING_ARRAY = new String[0];

    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Timer buildTimer(final String name, final MetricCustomized customized) {
        log.info("Timer: create {} with tags:{}", name, customized != null ? customized.tags : "");
        // 定义指标名称、标签、分位数
        final var builder = Timer.builder(name)
                .tags("hostname", HOSTNAME)
                .tags("ip", LOCAL_IP)
                .tags("ns", System.getenv("NACOS_NAMESPACE"))
                .tags("srv", System.getenv("NACOS_DATAID"))
                .publishPercentileHistogram();

        if (customized != null) {
            builder.description(customized.description);
            if (!customized.tags.isEmpty()) {
                builder.tags(customized.tags.toArray(EMPTY_STRING_ARRAY));
            }
            if (customized.minimumExpected != null) {
                builder.minimumExpectedValue(customized.minimumExpected);
            }
            if (customized.maximumExpected != null) {
                builder.maximumExpectedValue(customized.maximumExpected);
            }
        }

        return builder.register(meterRegistry);
    }

    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public DisposableGauge buildDisposableGauge(final Supplier<Number> f, final String name, final MetricCustomized customized) {
        log.info("DisposableGauge: create {} with tags:{}", name, customized != null ? customized.tags : "");
        final var builder = Gauge.builder(name, f)
                .tags("hostname", HOSTNAME)
                .tags("ip", LOCAL_IP)
                .tags("ns", System.getenv("NACOS_NAMESPACE"))
                .tags("srv", System.getenv("NACOS_DATAID"));

        if (customized != null) {
            builder.description(customized.description);
            if (!customized.tags.isEmpty()) {
                builder.tags(customized.tags.toArray(EMPTY_STRING_ARRAY));
            }
            if (!customized.baseUnit.isEmpty()) {
                builder.baseUnit(customized.baseUnit);
            }
        }

        final var gauge = builder.register(meterRegistry);
        return new DisposableGauge() {
            @Override
            public Gauge gauge() {
                return gauge;
            }

            @Override
            public void dispose() {
                meterRegistry.remove(gauge);
            }
        };
    }

    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Counter buildCounter(final String name, final MetricCustomized customized) {
        log.info("Counter: create {} with tags:{}", name, customized != null ? customized.tags : "");
        final var builder = Counter.builder(name)
                .tags("hostname", HOSTNAME)
                .tags("ip", LOCAL_IP)
                .tags("ns", System.getenv("NACOS_NAMESPACE"))
                .tags("srv", System.getenv("NACOS_DATAID"));

        if (customized != null) {
            builder.description(customized.description);
            if (!customized.tags.isEmpty()) {
                builder.tags(customized.tags.toArray(EMPTY_STRING_ARRAY));
            }
        }

        return builder.register(meterRegistry);
    }

    final MeterRegistry meterRegistry;
}
