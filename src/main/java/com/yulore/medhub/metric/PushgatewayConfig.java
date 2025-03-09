package com.yulore.medhub.metric;

import io.prometheus.client.exporter.PushGateway;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PushgatewayConfig {
    @Value("${prometheus.pushgateway.url}")
    private String pushgatewayUrl;

    @Bean
    public PushGateway pushGateway() {
        return new PushGateway(pushgatewayUrl);
    }
}