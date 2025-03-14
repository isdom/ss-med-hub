package com.yulore.metric;

import io.prometheus.client.exporter.PushGateway;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PushGatewayConfig {
    @Value("${prometheus.pushgateway.url}")
    private String pushgatewayUrl;

    @Bean
    public PushGateway pushGateway() {
        return new PushGateway(pushgatewayUrl);
    }
}