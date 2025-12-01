package com.yulore.ai.api;

import feign.Logger;
import feign.Request;
import lombok.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.TimeUnit;

@FeignClient(
        value = "${ds.api}",
        configuration = DeepSeekApi.ApiConfig.class
)
@ConditionalOnProperty(prefix = "ds", name = "api")
public interface DeepSeekApi {
    String AUTHORIZATION = "Authorization";
    String CONTENT_TYPE = "Content-Type";
    String ACCEPT = "Accept";

    String APP_JSON = "application/json";

    /*
    OkHttpClient client = new OkHttpClient().newBuilder()
            .build();
    MediaType mediaType = MediaType.parse("application/json");
    RequestBody body = RequestBody.create(mediaType,"
        {
            "messages": [
                {
                    "content": "You are a helpful assistant",
                    "role": "system"
                },
                {
                    "content": "Hi",
                    "role": "user"
                }
            ],
            "model": "deepseek-chat",
            "frequency_penalty": 0,
            "max_tokens": 4096,
            "presence_penalty": 0,
            "response_format": { "type": "text" },
            "stop": null,
            "stream": false,
            "stream_options": null,
            "temperature": 1,
            "top_p": 1,
            "tools": null,
            "tool_choice": "none",
            "logprobs": false,
            "top_logprobs": null
        }
    ");
    */
    @Builder
    @Data
    @ToString
    @NoArgsConstructor
    @AllArgsConstructor
    class Message {
        private String content;
        private String reasoning_content;
        private String role;
    }

    @Builder
    @Data
    @ToString
    class ResponseFormat {
        private String type;
    }

    ResponseFormat TEXT = ResponseFormat.builder().type("text").build();
    ResponseFormat JSON_OBJ = ResponseFormat.builder().type("json_object").build();

    @Builder
    @Data
    @ToString
    class CompletionsRequest {
        private Message[] messages;
        private String model;
        private Float frequency_penalty;
        private Integer max_tokens;
        private Float presence_penalty;
        private ResponseFormat response_format;
        private Object stop;
        private Boolean stream;
        private Float temperature;
        private Float top_p;
    }

    @Data
    @ToString
    class Completion {
        private int index;
        private String finish_reason;
        private Message message;
    }

    @Data
    @ToString
    class Usage {
        private int completion_tokens;
        private int prompt_tokens;
        private int prompt_cache_hit_tokens;
        private int prompt_cache_miss_tokens;
        private int total_tokens;
    }

    @Data
    @ToString
    class CompletionResponse {
        private String id;
        private Completion[] choices;
        private int created;
        private String model;
        private String system_fingerprint;
        private String object;
        private Usage usage;
    }
    /*
    Request request = new Request.Builder()
            .url("https://api.deepseek.com/chat/completions")
            .method("POST", body)
            .addHeader("Content-Type", "application/json")
            .addHeader("Accept", "application/json")
            .addHeader("Authorization", "Bearer <TOKEN>")
            .build();
    Response response = client.newCall(request).execute();
    */
    @RequestMapping(value = "/chat/completions",
            method = RequestMethod.POST)
    CompletionResponse completions(
            @RequestHeader(AUTHORIZATION) String authorization,
            @RequestHeader(CONTENT_TYPE) String contentType,
            @RequestHeader(ACCEPT) String acceptType,
            @RequestBody CompletionsRequest request
    );
    // 配置类定义
    class ApiConfig {
        @Bean
        public Request.Options options() {
            // connect(200ms), read(500ms), followRedirects(true)
            return new Request.Options(200, TimeUnit.MILLISECONDS,
                    500, TimeUnit.MILLISECONDS,
                    true);
        }

        @Bean
        Logger.Level feignLevel() {
            return switch (_logLevel.toUpperCase()) {
                case "NONE" -> Logger.Level.NONE;
                case "BASIC" -> Logger.Level.BASIC;
                case "HEADERS" -> Logger.Level.HEADERS;
                case "FULL" -> Logger.Level.FULL;
                default -> Logger.Level.NONE; // 默认使用NONE级别，避免无效配置导致问题
            };
        }

        @Value("${ds.api.log-level:NONE}")
        private String _logLevel;
    }
}
