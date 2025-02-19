package com.yulore.medhub.api;

import feign.Request;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.concurrent.TimeUnit;

@FeignClient(
        value = "${SCRIPT_PROVIDER:unknown-script}",
        configuration = ScriptApi.ScriptApiConfig.class
)
public interface ScriptApi {
    @RequestMapping(value = "${script.api.ai_reply:unknown_ai_api}", method = RequestMethod.GET)
    ApiResponse<AIReplyVO> ai_reply(
            @RequestParam("ccs_call_id")            String sessionId,
            @RequestParam("user_speech_text")       String userSpeechText,
            @RequestParam("idle_time")              Long idle_time, // in ms
            @RequestParam("is_speaking")            int is_speaking,
            @RequestParam("speaking_content_id")    String speaking_content_id,
            @RequestParam("speaking_duration_ms")   int speaking_duration_ms
            );

    @RequestMapping(value = "${script.api.content_report:unknown_report_content}", method = RequestMethod.GET)
    ApiResponse<Void> report_content(
            @RequestParam("ccs_call_id")            String sessionId,
            @RequestParam("content_id")             String content_id,
            @RequestParam("content_index")          int content_index,
            @RequestParam("speaker")                String speaker,
            @RequestParam("start_record_timestamp") long start_record_timestamp,
            @RequestParam("start_speak_timestamp")  long start_speak_timestamp,
            @RequestParam("stop_speak_timestamp")   long stop_speak_timestamp,
            @RequestParam("speak_duration")         long speak_duration
            );

    @RequestMapping(value = "${script.api.asr_report:unknown_report_asrtime}", method = RequestMethod.GET)
    ApiResponse<Void> report_asrtime(
            @RequestParam("ccs_call_id")            String sessionId,
            @RequestParam("content_id")             String content_id,
            @RequestParam("content_index")          int content_index,
            @RequestParam("sentence_begin_event_time") long begin_event_time,
            @RequestParam("sentence_end_event_time") long end_event_time
    );

    // 配置类定义
    class ScriptApiConfig {
        @Bean
        public Request.Options aiReplyOptions() {
            // connect(200ms), read(500ms), followRedirects(true)
            return new Request.Options(200, TimeUnit.MILLISECONDS,  500, TimeUnit.MILLISECONDS,true);
        }
    }}
