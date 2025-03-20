package com.yulore.metric;

import lombok.Builder;

import java.time.Duration;
import java.util.List;

@Builder
public class MetricCustomized {
    @Builder.Default
    public String description = "";

    @Builder.Default
    public List<String> tags = List.of();

    @Builder.Default
    public Duration minimumExpected = Duration.ofMillis(1);

    @Builder.Default
    public Duration maximumExpected = Duration.ofMillis(1000);
}
