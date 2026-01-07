package com.yulore.aliyun.api;

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
        value = "${dashscope.name}",
        url = "${dashscope.api.url}",
        path="/api/v1/services",
        configuration = DashScopeApi.Config.class
)
@ConditionalOnProperty(prefix = "dashscope.api", name = "url")
public interface DashScopeApi {
    @Data
    @ToString
    class Usage {
        public int  total_tokens;
    }

    @Data
    @ToString
    class DashScopeResponse<OUTPUT> {
        public int      status_code;
        public String   code;
        public String   message;
        public String   request_id;
        public OUTPUT   output;
        public Usage    usage;
    }

    /*
    '{
        "model": "text-embedding-v4",
        "input": {
            "texts": [
              "风急天高猿啸哀",
              "渚清沙白鸟飞回",
              "无边落木萧萧下",
              "不尽长江滚滚来"
            ]
        },
        "parameters": {
              "dimension": 1024,
              "output_type": "dense"
        }
    }'
    */
    @Builder
    @Data
    @ToString
    class TextEmbeddingInput{
        public String[] texts;
    }

    @Builder
    @Data
    @ToString
    class TextEmbeddingParameters {
        // default: 1024
        public Integer dimension;
        // default: "dense"
        public String output_type;
    }

    @Builder
    @Data
    @ToString
    class TextEmbeddingRequest {
        public String   model;
        public TextEmbeddingInput input;
        public TextEmbeddingParameters parameters;
    }

    /* REF： https://help.aliyun.com/zh/model-studio/text-embedding-synchronous-api
    {   "status_code": 200,
        "request_id": "1ba94ac8-e058-99bc-9cc1-7fdb37940a46",
        "code": "",
        "message": "",
        "output":{
            "embeddings": [
              {
                 "sparse_embedding":[
                   {"index":7149,"value":0.829,"token":"风"},
                   .....
                   {"index":111290,"value":0.9004,"token":"哀"}],
                 "embedding": [-0.006929283495992422,-0.005336422007530928, ...],
                 "text_index": 0
              },
              {
                 "sparse_embedding":[
                   {"index":246351,"value":1.0483,"token":"渚"},
                   .....
                   {"index":2490,"value":0.8579,"token":"回"}],
                 "embedding": [-0.006929283495992422,-0.005336422007530928, ...],
                 "text_index": 1
              },
              {
                 "sparse_embedding":[
                   {"index":3759,"value":0.7065,"token":"无"},
                   .....
                   {"index":1130,"value":0.815,"token":"下"}],
                 "embedding": [-0.006929283495992422,-0.005336422007530928, ...],
                 "text_index": 2
              },
              {
                 "sparse_embedding":[
                   {"index":562,"value":0.6752,"token":"不"},
                   .....
                   {"index":1589,"value":0.7097,"token":"来"}],
                 "embedding": [-0.001945948973298072,-0.005336422007530928, ...],
                 "text_index": 3
              }
            ]
        },
        "usage":{
            "total_tokens":27
        }
    }
    */
    @Data
    @ToString
    class TextEmbeddingResult {
        public float[]  embedding;
        public int      text_index;
    }

    @Data
    @ToString
    class TextEmbeddings {
        public TextEmbeddingResult[]  embeddings;
    }

    // REF: https://help.aliyun.com/document_detail/2510317.html
    @RequestMapping(
            value = "/embeddings/text-embedding/text-embedding",
            method = RequestMethod.POST,
            headers={"Content-Type=application/json",
                    "Accept=application/json",
                    "Authorization=Bearer ${dashscope.auth.token}"
            })
    DashScopeResponse<TextEmbeddings> textEmbedding(@RequestBody TextEmbeddingRequest request);

    // 配置类定义
    class Config {
         @Bean
        public Request.Options options() {
             return new Request.Options(
                     _connectTimeout, TimeUnit.MILLISECONDS,
                     _readTimeout, TimeUnit.MILLISECONDS,
                     // followRedirects(true)
                     true);
        }

        @Bean
        Logger.Level feignLevel(){
            return switch (_logLevel.toUpperCase()) {
                case "NONE" -> Logger.Level.NONE;
                case "BASIC" -> Logger.Level.BASIC;
                case "HEADERS" -> Logger.Level.HEADERS;
                case "FULL" -> Logger.Level.FULL;
                default -> Logger.Level.NONE; // 默认使用NONE级别，避免无效配置导致问题
            };
        }

        @Value("${dashscope.cfg.log-level:NONE}")
        private String _logLevel;

        // connect(200 ms)
        @Value("${dashscope.cfg.connect-timeout-ms:200}")
        private long _connectTimeout;

        // read(60 minutes)
        @Value("${dashscope.cfg.read-timeout-ms:3600000}")
        private long _readTimeout;
    }
}