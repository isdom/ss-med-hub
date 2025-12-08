package com.yulore.medhub.api;

import feign.Request;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.concurrent.TimeUnit;

@FeignClient(
        value = "${script.srv}",
        configuration = ScriptApi.Config.class
)
@ConditionalOnProperty(prefix = "script", name = "srv")
public interface ScriptApi {
    @Data
    @ToString
    class ReplyRequest {
        private String  ccs_call_id;
        private Integer user_speech_idx;
        private String  user_speech_text="";
        private Integer is_speaking=0;
        private Long    idle_time=0L;
        private Integer speaking_duration_ms=-1;
        private Long    speaking_content_id=0L;
    }

    @RequestMapping(value = "${script.api.ai_reply}", method = RequestMethod.GET)
    ApiResponse<AIReplyVO> ai_reply(
            @RequestParam("ccs_call_id")            String sessionId,
            @RequestParam("user_speech_idx")        Integer speechIdx,
            @RequestParam("user_speech_text")       String speechText,
            @RequestParam("idle_time")              Long idle_time, // in ms
            @RequestParam("is_speaking")            int is_speaking,
            @RequestParam("speaking_content_id")    String speaking_content_id,
            @RequestParam("speaking_duration_ms")   int speaking_duration_ms
            );

    @RequestMapping(value = "${script.api.content_report}", method = RequestMethod.GET)
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

    @RequestMapping(value = "${script.api.asr_report}", method = RequestMethod.GET)
    ApiResponse<Void> report_asrtime(
            @RequestParam("ccs_call_id")            String sessionId,
            @RequestParam("content_id")             String content_id,
            @RequestParam("content_index")          int content_index,
            @RequestParam("sentence_begin_event_time") long begin_event_time,
            @RequestParam("sentence_end_event_time") long end_event_time
    );

    @Data
    @Builder
    @ToString
    public class Text2IntentRequest {
        private String  sessionId;
        private Integer speechIdx;
        private String  speechText;
    }

    @Data
    @ToString
    public class Text2IntentResult {
        private String intentCode;
        private String traceId;
    }

    @RequestMapping(value = "${script.api.ai_t2i}", method = RequestMethod.POST)
    ApiResponse<Text2IntentResult> ai_t2i(@RequestBody Text2IntentRequest request);

    @Data
    @Builder
    @ToString
    public class Intent2ReplyRequest {
        private String  sessionId;
        private Integer speechIdx;
        private String  speechText;
        private String  traceId;
        private String  intent;
        private Integer isSpeaking;
        private Long    speakingContentId;
        private Integer speakingDurationMs;
    }

    @RequestMapping(value = "${script.api.ai_i2r}", method = RequestMethod.POST)
    ApiResponse<AIReplyVO> ai_i2r(@RequestBody Intent2ReplyRequest request);

    @Builder
    @Data
    @ToString
    class ExampleSentence {
        public int      index;
        public float    confidence;
        public String   id;
        public String   intentionCode;
        public String   intentionName;
        public String   text;
    }

    @Builder
    @Data
    @ToString
    class ESRequest {
        public String session_id;
        public String content_id;
        public int content_index;
        public String qa_id;
        public ExampleSentence[] es;
        public int  embedding_cost;
        public int  db_cost;
        public int  total_cost;
    }

    @RequestMapping(value = "${script.api.report_es}", method = RequestMethod.POST)
    ApiResponse<Void> report_es(@RequestBody ESRequest request);

    // 配置类定义
    class Config {
        @Bean
        public Request.Options options() {
            // connect(200ms), read(500ms), followRedirects(true)
            return new Request.Options(200, TimeUnit.MILLISECONDS,  500, TimeUnit.MILLISECONDS,true);
        }
    }}
