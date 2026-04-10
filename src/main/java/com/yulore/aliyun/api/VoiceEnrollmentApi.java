package com.yulore.aliyun.api;

import feign.Logger;
import feign.Request;
import feign.RequestInterceptor;
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

import java.util.List;
import java.util.concurrent.TimeUnit;

// REF-DOC: https://help.aliyun.com/zh/model-studio/cosyvoice-clone-design-api
@FeignClient(
        value = "${voiceenrollment.name}",
        url = "${voiceenrollment.api.url}",
        path="/api/v1/services/audio/tts/customization",
        configuration = VoiceEnrollmentApi.Config.class
)
@ConditionalOnProperty(prefix = "voiceenrollment.api", name = "url")
public interface VoiceEnrollmentApi {
    @Data
    @ToString
    class Usage {
        public int  count;
    }

    @Data
    @ToString
    class VoiceEnrollmentResponse<OUTPUT> {
        public String   request_id;
        public Usage    usage;
        public OUTPUT   output;
    }

    @Builder
    @Data
    @ToString
    class CreateVoiceInput {
        // 操作类型，固定为create_voice
        @Builder.Default
        public String   action = "create_voice";

        // 驱动音色的语音合成模型（参见支持的模型）。
        //必须与后续调用语音合成接口时使用的语音合成模型一致，否则合成会失败。
        public String   target_model;

        //重要
        // 仅在声音复刻时必须使用该参数
        // 用于复刻音色的音频文件URL，要求公网可访问。
        // 音频格式说明请参见。
        // 如何录音请参见录音操作指南。
        public String url;

        // 为音色指定一个便于识别的名称（仅允许数字和英文字母，不超过10个字符）。建议选用与角色、场景相关的标识。
        //该关键字会在设计的音色名中出现，例如关键字为“announcer”，最终音色名为：
        //声音复刻：cosyvoice-v3.5-plus-announcer-8aae0c0397fa408ca60c29cf******
        //声音设计：cosyvoice-v3.5-plus-vd-announcer-8aae0c0397fa408ca60c29cf******
        public String prefix;

        // 重要
        //仅在声音复刻场景下可以使用该参数
        //时间越长效果越好，较好的还原度至少需要20秒以上音频
        //音频预处理后用于声音复刻的参考音频最大时长，单位为秒。仅适用于 cosyvoice-v3.5-plus、cosyvoice-v3.5-flash和cosyvoice-v3-flash 模型。
        //取值范围：[3.0, 30.0]。
        @Builder.Default
        public float max_prompt_audio_length = 10.0f;

        // 重要
        //仅在声音复刻场景下可以使用该参数
        //若有背景噪音，建议开启，否则合成的断句处会出现噪音
        //安静环境建议关闭，以最大情况还原音色
        //是否开启音频预处理。开启后系统会在复刻前对输入音频进行降噪、音频增强、音量规整等处理。仅适用于 cosyvoice-v3.5-plus、cosyvoice-v3.5-flash和cosyvoice-v3-flash 模型。
        //取值范围：
        //true：开启
        //false：关闭
        @Builder.Default
        public boolean enable_preprocess = false;
    }

    @Builder
    @Data
    @ToString
    class CreateVoiceRequest {
        // 声音复刻/设计模型，固定为voice-enrollment。
        @Builder.Default
        public String model = "voice-enrollment";
        public CreateVoiceInput input;
    }

    @Data
    @ToString
    class CreateVoiceOutput {
        public String voice_id;
        public String target_model;
    }

    @RequestMapping(method = RequestMethod.POST)
    VoiceEnrollmentResponse<CreateVoiceOutput> createVoice(@RequestBody CreateVoiceRequest request);

    @Builder
    @Data
    @ToString
    class ListVoiceInput {
        // 操作类型，固定为list_voice。
        @Builder.Default
        public String   action = "list_voice";

        // 和创建音色时使用的prefix相同。仅允许数字和英文字母，不超过10个字符。
        public String prefix;

        // 页码索引，需大于或等于0。
        @Builder.Default
        public int page_index = 0;

        @Builder.Default
        public int page_size = 1000;
    }

    @Builder
    @Data
    @ToString
    class ListVoiceRequest {
        // 声音复刻/设计模型，固定为voice-enrollment。
        @Builder.Default
        public String model = "voice-enrollment";
        public ListVoiceInput input;
    }

    @Data
    @ToString
    class VoiceMeta {
        public String voice_id;
        public String target_model;
        public String gmt_create;
        public String gmt_modified;
        public String resource_link;
        public String status;
    }

    @Data
    @ToString
    class ListVoiceOutput {
        public List<VoiceMeta> voice_list;
    }

    @RequestMapping(method = RequestMethod.POST)
    VoiceEnrollmentResponse<ListVoiceOutput> listVoice(@RequestBody ListVoiceRequest request);

    @Builder
    @Data
    @ToString
    class QueryVoiceInput {
        // 操作类型，固定为query_voice。
        @Builder.Default
        public String   action = "query_voice";

        // 待查询的音色ID。
        public String voice_id;
    }

    @Builder
    @Data
    @ToString
    class QueryVoiceRequest {
        // 声音复刻/设计模型，固定为voice-enrollment。
        @Builder.Default
        public String model = "voice-enrollment";
        public QueryVoiceInput input;
    }

    @RequestMapping(method = RequestMethod.POST)
    VoiceEnrollmentResponse<VoiceMeta> queryVoice(@RequestBody QueryVoiceRequest request);

    @Builder
    @Data
    @ToString
    class UpdateVoiceInput {
        // 操作类型，固定为update_voice。
        @Builder.Default
        public String   action = "update_voice";

        // 待更新的音色。
        public String voice_id;

        //用于更新音色的音频文件URL。该URL要求公网可访问。
        public String url;
    }

    @Builder
    @Data
    @ToString
    class UpdateVoiceRequest {
        // 声音复刻/设计模型，固定为voice-enrollment。
        @Builder.Default
        public String model = "voice-enrollment";
        public UpdateVoiceInput input;
    }

    @RequestMapping(method = RequestMethod.POST)
    VoiceEnrollmentResponse<Void> updateVoice(@RequestBody UpdateVoiceRequest request);

    @Builder
    @Data
    @ToString
    class DeleteVoiceInput {
        // 操作类型，固定为delete_voice。
        @Builder.Default
        public String   action = "delete_voice";

        // 待删除的音色。
        public String voice_id;
    }

    @Builder
    @Data
    @ToString
    class DeleteVoiceRequest {
        // 声音复刻/设计模型，固定为voice-enrollment。
        @Builder.Default
        public String model = "voice-enrollment";
        public DeleteVoiceInput input;
    }

    @RequestMapping(method = RequestMethod.POST)
    VoiceEnrollmentResponse<Void> deleteVoice(@RequestBody DeleteVoiceRequest request);

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

        @Value("${voiceenrollment.cfg.log-level:NONE}")
        private String _logLevel;

        // connect(200 ms)
        @Value("${voiceenrollment.cfg.connect-timeout-ms:200}")
        private long _connectTimeout;

        // read(60 minutes)
        @Value("${voiceenrollment.cfg.read-timeout-ms:3600000}")
        private long _readTimeout;

        @Value("${voiceenrollment.api.key}")
        private String _api_key;

        @Bean
        public RequestInterceptor interceptor() {
            return template -> template
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .header("Authorization", "Bearer " + _api_key)
                    ;
        }
    }
}