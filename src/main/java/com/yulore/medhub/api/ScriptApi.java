package com.yulore.medhub.api;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

//@FeignClient(value = "${script.provider}")
@FeignClient(value = "${SCRIPT_PRPOVIDER}")
public interface ScriptApi {
    @RequestMapping(value = "${script.api.apply_session}", method = RequestMethod.GET)
    ApiResponse<ApplySessionVO> apply_session(
            @RequestParam("call_uuid")      String callUUID,
            @RequestParam("called_number")  String calledNumber
            );

    @RequestMapping(value = "${script.api.ai_reply}", method = RequestMethod.GET)
    ApiResponse<AIReplyVO> ai_reply(
            @RequestParam("ccs_call_id")            String sessionId,
            @RequestParam("user_speech_text")       String userSpeechText,
            @RequestParam("is_speaking")            int is_speaking,
            @RequestParam("idle_time")              Long idle_time // in ms
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
}
