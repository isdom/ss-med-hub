package com.yulore.chat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class BaiLianService {

    private final WebClient bailianWebClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public BaiLianService(WebClient bailianWebClient) {
        this.bailianWebClient = bailianWebClient;
    }

    // 构造请求体
    private Map<String, Object> buildChatRequest(final String model, final String userPrompt) {
        return Map.of(
                "model", model,
                "messages", List.of(Map.of("role", "user", "content", userPrompt)),
                "stream", true  // 开启流式
        );
    }

    // 提取content的辅助方法
    private String extractContent1(String rawResponse) {
        if (rawResponse == null || !rawResponse.startsWith("data:")) {
            return null;
        }
        String jsonData = rawResponse.substring(5).trim(); // 去掉 "data:" 前缀并trim
        if ("[DONE]".equals(jsonData)) {
            return null; // 流结束标记
        }
        try {
            JsonNode jsonNode = objectMapper.readTree(jsonData);
            // 提取增量内容
            return jsonNode.at("/choices/0/delta/content").asText(null);
        } catch (Exception e) {
            return null; // 解析失败，忽略该块
        }
    }

    // 提取 content，如果无效则返回 null（供 handle 使用）
    private String extractContent2(String rawResponse) {
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
            JsonNode jsonNode = objectMapper.readTree(jsonData);
            String content = jsonNode.at("/choices/0/delta/content").asText(null);
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
            JsonNode jsonNode = objectMapper.readTree(trimmed);
            String content = jsonNode.at("/choices/0/delta/content").asText("");
            // 忽略空内容（如第一个chunk的content为""）
            return content.isEmpty() ? null : content;
        } catch (Exception e) {
            log.warn("Failed to parse JSON chunk: {}", trimmed, e);
            return null;
        }
    }

    public Flux<String> streamChat(final String model, final String userPrompt) {
        return this.bailianWebClient.post()
                .accept(MediaType.TEXT_EVENT_STREAM)
                .bodyValue(buildChatRequest(model, userPrompt))
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