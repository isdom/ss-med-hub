package com.yulore.medhub.metric;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
public class MetricsRepo {
    @Bean("selectIfIdle")
    public AsyncTaskMetrics selectIfIdle() {
        return new AsyncTaskMetrics(meterRegistry, "nls.asr.selectidle.duration", "单个 ASRAgent checkAndSelectIfHasIdleAsync 执行时长");
    }

    @Bean("selectASRAgent")
    public AsyncTaskMetrics selectASRAgent() {
        return new AsyncTaskMetrics(meterRegistry, "nls.asr.selectagent.duration", "从 所有ASRAgent 中 selectASRAgent 执行时长");
    }

    final MeterRegistry meterRegistry;
}
