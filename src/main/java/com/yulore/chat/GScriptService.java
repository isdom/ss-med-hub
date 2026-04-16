package com.yulore.chat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class GScriptService {
    private final WebClient webClient;

    public GScriptService(@Value("${gscript.api.url}") String apiUrl) {
        webClient = WebClient.builder()
                .baseUrl(apiUrl)
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .build();
    }

    // 构造请求体
    private Map<String, Object> buildChatRequest(final List<ChatController.Item> contents) {
        return Map.of(
                "messages", contents,
                "stream", true  // 开启流式
        );
    }

    // 提取 content，如果无效则返回 null（供 handle 使用）
    private String extractContent1(String rawResponse) {
        if (rawResponse == null) {
            return null;
        }
        // 处理空行或注释行（如 ": heartbeat"）
        if (rawResponse.trim().isEmpty() || rawResponse.startsWith(":")) {
            return null;
        }
        if (!rawResponse.startsWith("data:")) {
            return null;  // 非数据行，忽略
        }
        String jsonData = rawResponse.substring(5).trim();
        if ("[DONE]".equals(jsonData)) {
            return null;  // 流结束标记，不输出内容
        }
        try {
            JsonNode jsonNode = new ObjectMapper().readTree(jsonData);
            String content = jsonNode.at("/choices/0/delta/content").asText("");
            return (content != null && !content.isEmpty()) ? content : null;
        } catch (Exception e) {
            log.warn("Failed to parse SSE data: {}", jsonData, e);
            return null;
        }
    }

    private String extractContent(String rawResponse) {
        if (rawResponse == null || rawResponse.trim().isEmpty()) {
            return null;
        }
        String trimmed = rawResponse.trim();
        // 结束标记
        if ("[DONE]".equals(trimmed)) {
            return null;
        }
        try {
            JsonNode jsonNode = new ObjectMapper().readTree(trimmed);
            String content = jsonNode.at("/choices/0/delta/content").asText("");
            // 忽略空内容（如第一个chunk的content为""）
            return content.isEmpty() ? null : content;
        } catch (Exception e) {
            log.warn("Failed to parse JSON chunk: {}", trimmed, e);
            return null;
        }
    }

    public Flux<String> streamChat(final List<ChatController.Item> contents) {
        return this.webClient.post()
                .accept(MediaType.TEXT_EVENT_STREAM)
                .bodyValue(buildChatRequest(contents))
                .retrieve()
                .bodyToFlux(String.class)
                .doOnNext(line -> log.debug("SSE line =>{}", line))
                .handle((rawResponse, sink) -> {
                    String content = extractContent(rawResponse);
                    if (content != null) {
                        sink.next(content);
                    }
                });
    }
}