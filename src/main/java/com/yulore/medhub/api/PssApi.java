package com.yulore.medhub.api;

import feign.Request;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.concurrent.TimeUnit;

@FeignClient(
        value = "${pss.name}",
        url = "${pss.api.url}",
        configuration = PssApi.PssApiConfig.class
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
    class PssApiConfig {
        @Bean
        public Request.Options pssOptions() {
            // connect(200ms), read(500ms), followRedirects(true)
            return new Request.Options(200, TimeUnit.MILLISECONDS,  500, TimeUnit.MILLISECONDS,true);
        }
    }
}
