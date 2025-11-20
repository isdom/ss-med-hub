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
        path="/v1/collections/${dv.collection}",
        configuration = DashVectorApi.Config.class
)
@ConditionalOnProperty(prefix = "dv.api", name = "url")
public interface DashVectorApi {

    String DASHVECTOR_AUTH_TOKEN = "dashvector-auth-token";
    String CONTENT_TYPE = "Content-Type";

    /*
    DashVector支持的数据类型
    当前DashVector支持Python的5种基础数据类型：
    str
    float
    int
    bool
    long
    */

    //@Builder
    @Data
    @ToString
    class Doc {
        public String   id;
        public float[]  vector;
        public Map<String, Object> fields;
        public Float score;
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
    @Data
    @ToString
    class InsertDocRequest {
        public Doc[] docs;
        public String partition;
    }

    @Data
    @ToString
    class DocOpResult {
        public String   doc_op;
        public String   id;
        public int      code;
        public String   message;
    }

    // REF: https://help.aliyun.com/document_detail/2510317.html
    @RequestMapping(
            value = "/docs",
            method = RequestMethod.POST,
            headers={"Content-Type=application/json",
                    "Accept=application/json",
                    "dashvector-auth-token=${dv.api.token}"
            })
    DVResponse<DocOpResult> insertDoc(@RequestBody InsertDocRequest request);

    // REF: https://help.aliyun.com/document_detail/2510321.html
    @RequestMapping(
            value = "/docs",
            method = RequestMethod.PUT,
            headers={"Content-Type=application/json",
                    "Accept=application/json",
                    "dashvector-auth-token=${dv.api.token}"
            })
    DVResponse<DocOpResult[]> updateDoc(@RequestBody InsertDocRequest request);

    @Builder
    @Data
    @ToString
    class DeleteDocRequest {
        public String[] ids;
        public String   partition;
        public Boolean  delete_all;
    }

    // REF: https://help.aliyun.com/document_detail/2510325.html
    @RequestMapping(
            value = "/docs",
            method = RequestMethod.DELETE,
            headers={"Content-Type=application/json",
                    "Accept=application/json",
                    "dashvector-auth-token=${dv.api.token}"
            })
    DVResponse<DocOpResult[]> deleteDoc(@RequestBody DeleteDocRequest request);

    @Builder
    @Data
    @ToString
    class QueryDocRequest {
        public String id;
        public float[] vector;
        public String filter;
        public String partition;
    }

    // REF: https://help.aliyun.com/document_detail/2510319.html
    //      https://help.aliyun.com/document_detail/2513006.html
    @RequestMapping(
            value = "/query",
            method = RequestMethod.POST,
            headers={"Content-Type=application/json",
                    "Accept=application/json",
                    "dashvector-auth-token=${dv.api.token}"
            })
    DVResponse<Doc> queryDoc(@RequestBody QueryDocRequest request);

    @Builder
    @Data
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
            value = "/query_group_by",
            method = RequestMethod.POST,
            headers={"Content-Type=application/json",
                    "Accept=application/json",
                    "dashvector-auth-token=${dv.api.token}"
            })
    DVResponse<Group> queryDocGroupBy(@RequestBody QueryDocGroupByRequest request);

    // REF: https://help.aliyun.com/document_detail/2510324.html
    @RequestMapping(
            value = "/v1/collections/{collection}/docs",
            method = RequestMethod.GET)
    DVResponse<Map<String, Doc>> fetchDoc(
            @RequestHeader(DASHVECTOR_AUTH_TOKEN) String authToken,
            @RequestHeader(CONTENT_TYPE) String contentType,
            @PathVariable("collection") String collection,
            @RequestParam("ids") String ids,
            @RequestParam("partition") String partition);

    // REF: https://help.aliyun.com/document_detail/2510328.html
    @RequestMapping(
            value = "/v1/collections/{collection}/partitions",
            method = RequestMethod.GET)
    DVResponse<String> listPartitions(
            @RequestHeader(DASHVECTOR_AUTH_TOKEN) String authToken,
            @RequestHeader(CONTENT_TYPE) String contentType,
            @PathVariable("collection") String collection);

    // REF: https://help.aliyun.com/document_detail/2510298.html
    @RequestMapping(
            value = "/v1/collections",
            method = RequestMethod.GET)
    DVResponse<String> listCollections(
            @RequestHeader(DASHVECTOR_AUTH_TOKEN) String authToken,
            @RequestHeader(CONTENT_TYPE) String contentType);

    @Data
    @ToString
    class CollectionMeta {
        public String   name;
        public int      dimension;
        public String   dtype;
        public String   metric;
        public String   status;
        public Map<String, String> fields_schema;
        public Map<String, String> partitions;
    }

    // REF: https://help.aliyun.com/document_detail/2510294.html
    @RequestMapping(
            value = "/v1/collections/{collection}",
            method = RequestMethod.GET)
    DVResponse<CollectionMeta> descCollection(
            @RequestHeader(DASHVECTOR_AUTH_TOKEN) String authToken,
            @RequestHeader(CONTENT_TYPE) String contentType,
            @PathVariable("collection") String collection);

    @Data
    @ToString
    class PartitionStats {
        public int      total_doc_count;
    }

    @Builder
    @Data
    @ToString
    class CreatePartitionRequest {
        private String name;
    }

    // REF: https://help.aliyun.com/document_detail/2510326.html
    @RequestMapping(
            value = "/v1/collections/{collection}/partitions",
            method = RequestMethod.POST)
    DVResponse<Void> createPartition(
            @RequestHeader(DASHVECTOR_AUTH_TOKEN) String authToken,
            @RequestHeader(CONTENT_TYPE) String contentType,
            @PathVariable("collection") String collection,
            @RequestBody CreatePartitionRequest request
    );


    // REF: https://help.aliyun.com/document_detail/2510327.html
    @RequestMapping(
            value = "/v1/collections/{collection}/partitions/{partition}",
            method = RequestMethod.GET)
    DVResponse<String> describePartition(
            @RequestHeader(DASHVECTOR_AUTH_TOKEN) String authToken,
            @RequestHeader(CONTENT_TYPE) String contentType,
            @PathVariable("collection") String collection,
            @PathVariable("partition") String partition
    );

    // REF: https://help.aliyun.com/document_detail/2510329.html
    @RequestMapping(
            value = "/v1/collections/{collection}/partitions/{partition}/stats",
            method = RequestMethod.GET)
    DVResponse<PartitionStats> statsPartition(
            @RequestHeader(DASHVECTOR_AUTH_TOKEN) String authToken,
            @RequestHeader(CONTENT_TYPE) String contentType,
            @PathVariable("collection") String collection,
            @PathVariable("partition") String partition
    );

    @Data
    @ToString
    class CollectionStats {
        public int      total_doc_count;
        public float    index_completeness;
        public Map<String, PartitionStats> partitions;
    }

    // REF: https://help.aliyun.com/document_detail/2510304.html
    @RequestMapping(
            value = "/v1/collections/{collection}/stats",
            method = RequestMethod.GET)
    DVResponse<CollectionStats> statsCollection(
            @RequestHeader(DASHVECTOR_AUTH_TOKEN) String authToken,
            @RequestHeader(CONTENT_TYPE) String contentType,
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
