package com.yulore.medhub.api;

import com.yulore.znc.vo.RemovePhraseResult;
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
    @Builder
    @Data
    @ToString
    class UserSpeechResult {
        public RemovePhraseResult result;
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

    // 配置类定义
    class Config {
        @Bean
        public Request.Options options() {
            // connect(200ms), read(500ms), followRedirects(true)
            return new Request.Options(200, TimeUnit.MILLISECONDS,  500, TimeUnit.MILLISECONDS,true);
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
    }
}
