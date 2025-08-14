package com.yulore.medhub.ws.actor;

import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@ConfigurationProperties(prefix = "intent")
@Data
@ToString
public class IntentConfig {
    private String prefix = "INVALID";
    private List<String> ring0 = List.of();
}
