package com.yulore.medhub.ws;

import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "ws")
@ToString
@Setter
public class WsConfigProperties {
    public String host;
    public int port;
    public Map<String, String> pathMappings = new HashMap<>();
}
