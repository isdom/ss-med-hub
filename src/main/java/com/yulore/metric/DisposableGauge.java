package com.yulore.metric;

import io.micrometer.core.instrument.Gauge;

public interface DisposableGauge {
    Gauge gauge();
    void dispose();
}
