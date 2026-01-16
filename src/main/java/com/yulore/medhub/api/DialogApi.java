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
        value = "${dialog.srv}",
        configuration = DialogApi.Config.class
)
@ConditionalOnProperty(prefix = "dialog", name = "srv")
public interface DialogApi {
    @Data
    @ToString
    class UserSpeechResult {
        public String result;
    }

    @Builder
    @Data
    @ToString
    class UserSpeechRequest {
        public String   sessionId;
        public Integer  botId;
        public Long     nodeId;
        public String   qa_id;
        public Long     userContentId;
        public String   speechText;
    }

    @RequestMapping(value = "${dialog.api.user_speech}", method = RequestMethod.POST)
    ApiResponse<UserSpeechResult> user_speech(@RequestBody UserSpeechRequest request);

    @Data
    @ToString
    class ClassifySpeechResult {
        public String ins;
    }

    @Builder
    @Data
    @ToString
    class ClassifySpeechRequest {
        public String   esl;
        public String   sessionId;
        public Integer  botId;
        public Long     nodeId;
        public String   scriptText;
        public String   speechText;
    }

    @RequestMapping(value = "${dialog.api.classify_speech}", method = RequestMethod.POST)
    ApiResponse<ClassifySpeechResult> classify_speech(@RequestBody final ClassifySpeechRequest request);

    @Data
    @ToString
    class EsMatchResult {
        // classify info
        public String oldIntent;
        public Integer[] intents;

        public EsMatchContext[]  contexts;
    }

    @Data
    @ToString
    class EsMatchContext {
        // speech info
        public String sessionId;
        public Long   contentId;
        public String speechText;

        // esl info
        public String esl;
        public String dvId;
        public Float score;

        // classify info
        public Integer[] intents;
    }

    @RequestMapping(value = "${dialog.api.speech2intent}", method = RequestMethod.POST)
    ApiResponse<EsMatchResult> speech2intent(@RequestBody final ClassifySpeechRequest request);

    @RequestMapping(value = "${dialog.api.report_s2i}", method = RequestMethod.POST)
    ApiResponse<Void> report_s2i(@RequestBody final EsMatchContext[] request);

    // 配置类定义
    class Config {
        @Bean
        public Request.Options options() {
            // connect(200ms), read(500ms), followRedirects(true)
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

        @Value("${dialog.api.log-level:NONE}")
        private String _logLevel;

        @Value("${dialog.api.connect-timeout-ms:200}")
        private long _connectTimeout;

        @Value("${dialog.api.read-timeout-ms:30000}")
        private long _readTimeout;
    }
}
