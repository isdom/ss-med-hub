package com.yulore.aliyun.dashvector;

import feign.Request;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@FeignClient(
        value = "${dv.name}",
        url = "${dv.api.url}",
        configuration = DashVectorApi.Config.class
)
@ConditionalOnProperty(prefix = "dv.api", name = "url")
public interface DashVectorApi {
    @Data
    @ToString
    class Doc {
        public String   id;
        public float[]  vector;
        public Map<String, Object> fields;
        public float score;
    }

    @Data
    @ToString
    class DVResponse<OUTPUT> {
        public int      code;
        public String   message;
        public String   request_id;
        public OUTPUT[] output;
    }

    static <R> DVResponse<R> emptyResponse() {
        final var resp = new DVResponse<R>();
        resp.output = null;
        return resp;
    }

    @Builder
    @ToString
    class QueryDocRequest {
        public String id;
        public float[] vector;
        public String filter;
        public String partition;
    }

    // REF: https://help.aliyun.com/document_detail/2510319.html
    @RequestMapping(
            value = "/v1/collections/{collection}/query",
            method = RequestMethod.POST)
    DVResponse<Doc> queryDoc(
            @RequestHeader("dashvector-auth-token") String authToken,
            @RequestHeader("Content-Type") String contentType,
            @PathVariable("collection") String collection,
            @RequestBody QueryDocRequest request);

    @Builder
    @ToString
    class QueryDocGroupByRequest {
        public String id;
        public float[] vector;
        public String group_by_field;
        public Integer group_topk;
        public Integer group_count;
        public String filter;
        public String partition;
        public Boolean include_vector;
    }

    @Data
    @ToString
    class Group {
        public String   group_id;
        public Doc[]    docs;
    }

    // REF: https://help.aliyun.com/document_detail/2715274.html
    @RequestMapping(
            value = "/v1/collections/{collection}/query_group_by",
            method = RequestMethod.POST)
    DVResponse<Group> queryDocGroupBy(
            @RequestHeader("dashvector-auth-token") String authToken,
            @RequestHeader("Content-Type") String contentType,
            @PathVariable("collection") String collection,
            @RequestBody QueryDocGroupByRequest request);

    // REF: https://help.aliyun.com/document_detail/2510328.html
    @RequestMapping(
            value = "/v1/collections/{collection}/partitions",
            method = RequestMethod.GET)
    DVResponse<String> getPartitions(
            @RequestHeader("dashvector-auth-token") String authToken,
            @RequestHeader("Content-Type") String contentType,
            @PathVariable("collection") String collection);

    // REF: https://help.aliyun.com/document_detail/2510298.html
    @RequestMapping(
            value = "/v1/collections",
            method = RequestMethod.GET)
    DVResponse<String> getCollections(
            @RequestHeader("dashvector-auth-token") String authToken,
            @RequestHeader("Content-Type") String contentType);

    @Data
    @ToString
    class CollectionMeta {
        public String   name;
        public int dimension;
        public String dtype;
        public String metric;
        public String status;
        public Map<String, String> fields_schema;
        public Map<String, String> partitions;
    }

    // REF: https://help.aliyun.com/document_detail/2510294.html
    @RequestMapping(
            value = "/v1/collections/{collection}",
            method = RequestMethod.GET)
    DVResponse<CollectionMeta> descCollection(
            @RequestHeader("dashvector-auth-token") String authToken,
            @RequestHeader("Content-Type") String contentType,
            @PathVariable("collection") String collection);

    // 配置类定义
    class Config {
        @Bean
        public Request.Options options() {
            // connect(200ms), read(500ms), followRedirects(true)
            return new Request.Options(200, TimeUnit.MILLISECONDS,  500, TimeUnit.MILLISECONDS,true);
        }
    }
}
