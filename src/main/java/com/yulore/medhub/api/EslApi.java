package com.yulore.medhub.api;

import feign.Request;
import lombok.Data;
import lombok.ToString;
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
        configuration = EslApi.EslApiConfig.class
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
        public int  searchDuration;
        public String   vendorName;
        public String   requestId;
        public String   code;
        public String   message;
    }

    @Data
    @ToString
    class SearchResponse {
        public String   code;
        public String   message;
        public Result[] result;
        public Develop  dev;
    }

    @RequestMapping(value = "${esl.api.search_text}", method = RequestMethod.GET)
    SearchResponse search_text(@RequestHeader Map<String, String> headers, @RequestParam("text") String text);

    // 配置类定义
    class EslApiConfig {
        @Bean
        public Request.Options eslOptions() {
            // connect(200ms), read(500ms), followRedirects(true)
            return new Request.Options(200, TimeUnit.MILLISECONDS,  500, TimeUnit.MILLISECONDS,true);
        }
    }
}
