package com.yulore.medhub.vo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

// 1. 全局配置: 通过 ObjectMapper 的 DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES 参数，设置全局忽略未知属性
// 2. 基于注解的局部配置: 在 Java 类上添加 @JsonIgnoreProperties(ignoreUnknown = true) 注解，仅对特定类生效
@Slf4j
@Data
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class WSCommandVO<PAYLOAD> {
    public static final TypeReference<WSCommandVO<Void>> WSCMD_VOID = new TypeReference<>() {};
    public static <PAYLOAD> WSCommandVO<PAYLOAD> parse(final String message, final TypeReference<WSCommandVO<PAYLOAD>> type) throws JsonProcessingException {
        return new ObjectMapper().readValue(message, type);
    }

    public Map<String, String> header;
    // Map<String, String> payload;
    public PAYLOAD payload;
}
