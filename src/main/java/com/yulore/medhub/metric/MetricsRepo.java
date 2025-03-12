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
//    @Bean("selectIdleASR")
//    public AsyncTaskMetrics selectIdleASR() {
//        return new AsyncTaskMetrics(meterRegistry, "nls.asr.idle.select.duration", "单个 ASRAgent checkAndSelectIfHasIdleAsync 执行时长");
//    }

    @Bean("selectASRAgent")
    public AsyncTaskMetrics selectASRAgent() {
        return new AsyncTaskMetrics(meterRegistry, "nls.asr.agent.select.duration", "从 所有 ASRAgent 中 selectASRAgent 执行时长");
    }

//    @Bean("selectIdleTxASR")
//    public AsyncTaskMetrics selectIdleTxASR() {
//        return new AsyncTaskMetrics(meterRegistry, "nls.txasr.idle.select.duration", "单个 TxASRAgent checkAndSelectIfHasIdleAsync 执行时长");
//    }

    @Bean("selectTxASRAgent")
    public AsyncTaskMetrics selectTxASRAgent() {
        return new AsyncTaskMetrics(meterRegistry, "nls.txasr.agent.select.duration", "从 所有 TxASRAgent 中 selectTxASRAgent 执行时长");
    }

//    @Bean("selectIdleTTS")
//    public AsyncTaskMetrics selectIdleTTS() {
//        return new AsyncTaskMetrics(meterRegistry, "nls.tts.idle.select.duration", "单个 TTSAgent checkAndSelectIfHasIdleAsync 执行时长");
//    }

    @Bean("selectTTSAgent")
    public AsyncTaskMetrics selectTTSAgent() {
        return new AsyncTaskMetrics(meterRegistry, "nls.tts.agent.select.duration", "从 所有 TTSAgent 中 selectTTSAgent 执行时长");
    }

//    @Bean("selectIdleCosy")
//    public AsyncTaskMetrics selectIdleCosy() {
//        return new AsyncTaskMetrics(meterRegistry, "nls.cosy.idle.select.duration", "单个 CosyAgent checkAndSelectIfHasIdleAsync 执行时长");
//    }

    @Bean("selectCosyAgent")
    public AsyncTaskMetrics selectCosyAgent() {
        return new AsyncTaskMetrics(meterRegistry, "nls.cosy.agent.select.duration", "从 所有 CosyAgent 中 selectCosyAgent 执行时长");
    }

    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Timer build(final String name, final String desc, final String[] tags) {
        return new AsyncTaskMetrics(meterRegistry, name, desc, tags).getTimer();
    }

    final MeterRegistry meterRegistry;
}
