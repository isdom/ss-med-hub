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
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.util.concurrent.TimeUnit;

@FeignClient(
        value = "${call.srv}",
        configuration = CallApi.Config.class
)
@ConditionalOnProperty(prefix = "call", name = "srv")
public interface CallApi {
    @Builder
    @Data
    @ToString
    class ApplySessionRequest {
        private String tid;
        private String uuid;
    }

    @RequestMapping(value = "${call.api.apply_session}", method = RequestMethod.POST)
    ApiResponse<ApplySessionVO> apply_session(@RequestBody ApplySessionRequest request);

    @Builder
    @Data
    @ToString
    class MockAnswerRequest {
        private String sessionId;
        private String uuid;
        private String tid;
        private long answerTime;
    }
    @RequestMapping(value = "${call.api.mock_answer}", method = RequestMethod.POST)
    ApiResponse<UserAnswerVO> mock_answer(@RequestBody MockAnswerRequest request);

    @Builder
    @Data
    @ToString
    class UserAnswerRequest {
        private String sessionId;
        private String kid;
        private String tid;
        private String realName;
        private String genderStr;
        private String aesMobile;
        private long answerTime;
    }
    @RequestMapping(value = "${call.api.user_answer}", method = RequestMethod.POST)
    ApiResponse<UserAnswerVO> user_answer(@RequestBody UserAnswerRequest request);

    // 配置类定义
    class Config {
        @Bean
        public Request.Options options() {
            // connect(200ms), read(10 seconds), followRedirects(true)
            return new Request.Options(200, TimeUnit.MILLISECONDS, 10, TimeUnit.SECONDS, true);
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

        @Value("${call.api.log-level:NONE}")
        private String _logLevel;
    }
}
