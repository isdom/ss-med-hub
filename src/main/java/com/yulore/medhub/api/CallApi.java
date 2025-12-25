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

    @RequestMapping(value = "${call.api.ai_i2r}", method = RequestMethod.POST)
    ApiResponse<AIReplyVO> ai_i2r(@RequestBody Intent2ReplyRequest request);

    // 配置类定义
    class Config {
        @Bean
        public Request.Options options() {
            return new Request.Options(
                    _connectTimeout, TimeUnit.MILLISECONDS,
                    _readTimeout, TimeUnit.MILLISECONDS,
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

        @Value("${call.api.log-level:NONE}")
        private String _logLevel;

        @Value("${call.api.connect-timeout-ms:200}")
        private long _connectTimeout;

        @Value("${call.api.read-timeout-ms:500}")
        private long _readTimeout;
    }
}
