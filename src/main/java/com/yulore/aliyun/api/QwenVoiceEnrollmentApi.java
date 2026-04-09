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

// REF-DOC: https://help.aliyun.com/zh/model-studio/qwen-tts-voice-cloning
@FeignClient(
        value = "${qwen-ve.name}",
        url = "${qwen-ve.api.url}",
        path="/api/v1/services/audio/tts/customization",
        configuration = QwenVoiceEnrollmentApi.Config.class
)
@ConditionalOnProperty(prefix = "qwen-ve.api", name = "url")
public interface QwenVoiceEnrollmentApi {
    @Data
    @ToString
    class Usage {
        // 本次请求实际计入费用的“创建音色”次数，本次请求的费用为 count×0.01 元。
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
    class AudioData {
        public String   data;
    }

    @Builder
    @Data
    @ToString
    class CreateVoiceInput {
        // 操作类型，固定为create。
        @Builder.Default
        public String   action = "create";

        // 驱动音色的语音合成模型，支持的模型有（两类）：
        //千问3-TTS-VC-Realtime（参见实时语音合成-千问）：
        //qwen3-tts-vc-realtime-2026-01-15
        //qwen3-tts-vc-realtime-2025-11-27
        //千问3-TTS-VC（参见语音合成-千问）：
        //qwen3-tts-vc-2026-01-22
        //必须与后续调用语音合成接口时使用的语音合成模型一致，否则合成会失败。
        public String   target_model;

        // 为音色指定一个便于识别的名称（仅允许数字、大小写字母和下划线，不超过16个字符）。建议选用与角色、场景相关的标识。
        //该关键字会在复刻的音色名中出现，例如关键字为“guanyu”，最终音色名为“qwen-tts-vc-guanyu-voice-20250812105009984-838b”
        public String preferred_name;

        // 用于复刻的音频（录制时需遵循录音操作指南，音频需满足音频要求）。
        //可通过以下两种方式提交音频数据：
        //Data URL
        //格式：data:<mediatype>;base64,<data>
        //<mediatype>：MIME类型
        //WAV：audio/wav
        //MP3：audio/mpeg
        //M4A：audio/mp4
        //<data>：音频转成的Base64编码的字符串
        //Base64编码会增大体积，请控制原文件大小，确保编码后仍小于10MB
        //示例：data:audio/wav;base64,SUQzBAAAAAAAI1RTU0UAAAAPAAADTGF2ZjU4LjI5LjEwMAAAAAAAAAAAAAAA//PAxABQ/BXRbMPe4IQAhl9
        //音频URL（推荐将音频上传至OSS）
        //文件大小不超过10MB
        //URL必须公网可访问且无需鉴权
        public AudioData audio;
    }

    @Builder
    @Data
    @ToString
    class CreateVoiceRequest {
        // 声音复刻模型，固定为qwen-voice-enrollment
        @Builder.Default
        public String model = "qwen-voice-enrollment";
        public CreateVoiceInput input;
    }

    @Data
    @ToString
    class CreateVoiceOutput {
        public String voice;
        public String target_model;
    }

    // https://help.aliyun.com/zh/model-studio/qwen-tts-voice-cloning#1eaa57d82did9
    @RequestMapping(method = RequestMethod.POST)
    VoiceEnrollmentResponse<CreateVoiceOutput> createVoice(@RequestBody CreateVoiceRequest request);

    @Builder
    @Data
    @ToString
    class ListVoiceInput {
        // 操作类型，固定为list。
        @Builder.Default
        public String   action = "list";

        // 页码索引，需大于或等于0。
        @Builder.Default
        public int page_index = 0;

        @Builder.Default
        public int page_size = 100;
    }

    @Builder
    @Data
    @ToString
    class ListVoiceRequest {
        // 声音复刻模型，固定为qwen-voice-enrollment。
        @Builder.Default
        public String model = "qwen-voice-enrollment";
        public ListVoiceInput input;
    }

    @Data
    @ToString
    class VoiceMeta {
        public String voice;
        public String target_model;
        public String gmt_create;
    }

    @Data
    @ToString
    class ListVoiceOutput {
        public List<VoiceMeta> voice_list;
    }

    // https://help.aliyun.com/zh/model-studio/qwen-tts-voice-cloning#401d33226330i
    @RequestMapping(method = RequestMethod.POST)
    VoiceEnrollmentResponse<ListVoiceOutput> listVoice(@RequestBody ListVoiceRequest request);

    @Builder
    @Data
    @ToString
    class DeleteVoiceInput {
        // 操作类型，固定为delete。
        @Builder.Default
        public String   action = "delete";

        // 待删除的音色。
        public String voice;
    }

    @Builder
    @Data
    @ToString
    class DeleteVoiceRequest {
        // 声音复刻模型，固定为qwen-voice-enrollment。
        @Builder.Default
        public String model = "qwen-voice-enrollment";
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

        @Value("${qwen-ve.cfg.log-level:NONE}")
        private String _logLevel;

        // connect(200 ms)
        @Value("${qwen-ve.cfg.connect-timeout-ms:200}")
        private long _connectTimeout;

        // read(60 minutes)
        @Value("${qwen-ve.cfg.read-timeout-ms:3600000}")
        private long _readTimeout;

        @Value("${qwen-ve.api.key}")
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