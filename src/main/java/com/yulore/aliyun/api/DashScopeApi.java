package com.yulore.aliyun.api;

import feign.Logger;
import feign.Request;
import lombok.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.util.List;
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
        public Integer  input_tokens;
        public Integer  output_tokens;
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

    @Builder
    @Data
    @ToString
    @NoArgsConstructor
    @AllArgsConstructor
    class Message {
        private String content;
        private String role;
    }

    @Builder
    @Data
    @ToString
    class TextGenerationInput {
        private List<Message> messages;
    }

    @Builder
    @Data
    @ToString
    class ResponseFormat {
        // default: "text"
        // optional: json_object, json_schema
        public String type;
    }

    @Builder
    @Data
    @ToString
    class TextGenerationParameters {
        // （可选）
        // 采样温度，控制模型生成文本的多样性。
        // temperature越高，生成的文本更多样，反之，生成的文本更确定。
        // 取值范围： [0, 2)
        public Float temperature;
        // （可选）
        // 核采样的概率阈值，控制模型生成文本的多样性。
        // top_p越高，生成的文本更多样。反之，生成的文本更确定。
        // 取值范围：（0,1.0]。
        public Float top_p;
        // （可选）
        // 生成过程中采样候选集的大小。例如，取值为50时，仅将单次生成中得分最高的50个Token组成随机采样的候选集。
        // 取值越大，生成的随机性越高；取值越小，生成的确定性越高。
        // 取值为None或当top_k大于100时，表示不启用top_k策略，此时仅有top_p策略生效。
        // 取值需要大于或等于0。
        public Integer top_k;
        // （可选）
        // 使用混合思考模型时，是否开启思考模式，适用于 Qwen3 、Qwen3-VL模型。相关文档：深度思考
        // 可选值：
        // true：开启
        // 开启后，思考内容将通过reasoning_content字段返回。
        // false：不开启
        public Boolean enable_thinking;
        //（可选）
        // 思考过程的最大长度。适用于Qwen3-VL、Qwen3 的商业版与开源版模型。相关文档：限制思考长度。
        // 默认值为模型最大思维链长度，请参见：模型列表(https://help.aliyun.com/zh/model-studio/models)
        public Integer thinking_budget;
        // （可选）默认值为 false
        // 是否开启代码解释器功能。仅适用于思考模式下的 qwen3-max-preview。相关文档：代码解释器
        // 可选值：
        // true：开启
        // false：不开启
        public Boolean enable_code_interpreter;
        // （可选）
        // 模型生成时连续序列中的重复度。提高repetition_penalty时可以降低模型生成的重复度，1.0表示不做惩罚。
        // 没有严格的取值范围，只要大于0即可。
        public Float repetition_penalty;
        // （可选）
        // 控制模型生成文本时的内容重复度。
        // 取值范围：[-2.0, 2.0]。正值降低重复度，负值增加重复度。
        // 在创意写作或头脑风暴等需要多样性、趣味性或创造力的场景中，建议调高该值；
        // 在技术文档或正式文本等强调一致性与术语准确性的场景中，建议调低该值。
        public Float presence_penalty;
        // （可选）
        // 用于限制模型输出的最大 Token 数。若生成内容超过此值，生成将提前停止，且返回的finish_reason为length。
        // 默认值与最大值均为模型的最大输出长度，请参见文本生成-通义千问。
        // 适用于需控制输出长度的场景，如生成摘要、关键词，或用于降低成本、缩短响应时间。
        // 触发 max_tokens 时，响应的 finish_reason 字段为 length。
        public Integer max_tokens;
        //（可选）
        // 随机数种子。用于确保在相同输入和参数下生成结果可复现。若调用时传入相同的 seed 且其他参数不变，模型将尽可能返回相同结果。
        // 取值范围：[0,2^31−1]。
        public Integer seed;
        // default: {"type": "text"}
        public ResponseFormat response_format;
        //（可选） 默认为text（Qwen3-Max、Qwen3-VL、QwQ 模型、Qwen3 开源模型（除了qwen3-next-80b-a3b-instruct）与 Qwen-Long 模型默认值为 message）
        // 返回数据的格式。推荐您优先设置为message，可以更方便地进行多轮对话。
        public String result_format;
        // boolean （可选）默认值为 false
        // 是否返回输出 Token 的对数概率，可选值：
        // true
        // 返回
        // false
        // 不返回
        // 支持以下模型：
        // qwen-plus系列的快照模型（不包含主线模型）
        // qwen-turbo 系列的快照模型（不包含主线模型）
        // Qwen3 开源模型
        public Boolean logprobs;
        //（可选）默认值为0
        // 指定在每一步生成时，返回模型最大概率的候选 Token 个数。
        // 取值范围：[0,5]
        // 仅当 logprobs 为 true 时生效。
        public Integer top_logprobs;
        //（可选） 默认值为1
        // 生成响应的个数，取值范围是1-4。对于需要生成多个响应的场景（如创意写作、广告文案等），可以设置较大的 n 值。
        // 当前仅支持 qwen-plus、 Qwen3（非思考模式）、qwen-plus-character 模型，且在传入 tools 参数时固定为1。
        // 设置较大的 n 值不会增加输入 Token 消耗，会增加输出 Token 的消耗。
        public Integer n;
        //（可选）
        // 用于指定停止词。当模型生成的文本中出现stop 指定的字符串或token_id时，生成将立即终止。
        // 可传入敏感词以控制模型的输出。
        // stop为数组时，不可将token_id和字符串同时作为元素输入，比如不可以指定为["你好",104307]。
        public String stop;
        //（可选） 默认值为false
        // 模型在生成文本时是否使用互联网搜索结果进行参考。取值如下：
        // true：启用互联网搜索，模型会将搜索结果作为文本生成过程中的参考信息，但模型会基于其内部逻辑判断是否使用互联网搜索结果。
        // 若开启后未联网搜索，可优化提示词，或设置search_options中的forced_search参数开启强制搜索。
        // false：关闭互联网搜索。
        public Boolean enable_search;
    }

    @Builder
    @Data
    @ToString
    class TextGenerationRequest {
        private String model;
        private TextGenerationInput input;
        private TextGenerationParameters parameters;
    }

    @Data
    @ToString
    class TextGenerationChoice {
        private String finish_reason;
        private Message message;
    }

    @Data
    @ToString
    class TextGenerationResult {
        private TextGenerationChoice[]  choices;
    }

    // REF: https://help.aliyun.com/zh/model-studio/qwen-api-reference
    @RequestMapping(
            value = "/aigc/text-generation/generation",
            method = RequestMethod.POST,
            headers={"Content-Type=application/json",
                    "Accept=application/json",
                    "Authorization=Bearer ${dashscope.auth.token}"
            })
    DashScopeResponse<TextGenerationResult> textGeneration(@RequestBody TextGenerationRequest request);

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