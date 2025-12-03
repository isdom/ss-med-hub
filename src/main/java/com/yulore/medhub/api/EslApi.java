package com.yulore.medhub.api;

import feign.Logger;
import feign.Request;
import lombok.Data;
import lombok.ToString;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@FeignClient(
        value = "${esl.name}",
        url = "${esl.api.url}",
        configuration = EslApi.Config.class
)
@ConditionalOnProperty(prefix = "esl.api", name = "url")
public interface EslApi {
    @Data
    @ToString
    class Result {
        public String   clusterCode;
        public float    confidence;
        public String   intentionCode;
        public String   intentionName;
    }

    @Data
    @ToString
    class Develop {
        public int  embeddingDuration;
        public int  dbExecutionDuration;
        public String   vendorName;
        public String   requestId;
        public String   code;
        public String   message;
    }

    @Data
    @ToString
    class ExampleSentence {
        public String   id;
        public String   intentionCode;
        public String   intentionName;
        public String   text;
    }

    @Data
    @ToString
    class Hit {
        public float    confidence;
        public ExampleSentence  es;
    }

    @Data
    @ToString
    class EslResponse<RESULT> {
        public String   code;
        public String   message;
        public RESULT[] result;
        public Develop  dev;
    }

    static <R> EslResponse<R> emptyResponse() {
        final var resp = new EslResponse<R>();
        resp.result = null;
        return resp;
    }

    @RequestMapping(
            value = "${esl.api.search_text}",
            method = RequestMethod.GET)
    EslResponse<Hit> search_text(
            @RequestHeader Map<String, String> headers,
            @RequestParam("text") String text,
            @RequestParam("ct") float ct);

    @RequestMapping(
            //value = "/ref/search",
            value = "${esl.api.search_ref}",
            method = RequestMethod.GET)
    EslResponse<Hit> search_ref(
            @RequestHeader Map<String, String> headers,
            @RequestParam("text") String text,
            @RequestParam("ct") float ct);

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

        @Value("${esl.api.log-level:NONE}")
        private String _logLevel;
    }
}
