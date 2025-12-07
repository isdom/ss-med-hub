package com.yulore.medhub.api;

import feign.Logger;
import feign.Request;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.concurrent.TimeUnit;

@FeignClient(
        value = "${pss.name}",
        url = "${pss.api.url}",
        configuration = PssApi.Config.class
)
@ConditionalOnProperty(prefix = "pss.api", name = "url")
public interface PssApi {
    @Data
    @ToString
    class Message {
        public String   description;
    }

    @Data
    @ToString
    class VectorResult {
        public List<Double> vec;
    }

    @Data
    @ToString
    class SpkResponse {
        public boolean  success;
        public int      code;
        public Message  message;
        public VectorResult  result;
    }

    @Builder
    @Data
    @ToString
    class SpkRequest {
        public String   audio;

        @Builder.Default
        public String   task = "spk";

        @Builder.Default
        public String audio_format = "wav";

        @Builder.Default
        public int sample_rate = 16000;
    }

    @RequestMapping(value = "${pss.api.vector_spk}", method = RequestMethod.POST, consumes = "application/json")
    SpkResponse vector_spk(@RequestBody SpkRequest request);

    // 配置类定义
    class Config {
        @Bean
        public Request.Options options() {
            // connect(200 ms), read(60 minutes), followRedirects(true)
            return new Request.Options(200, TimeUnit.MILLISECONDS,  60, TimeUnit.MINUTES,true);
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

        @Value("${pss.api.log-level:NONE}")
        private String _logLevel;
    }
}
