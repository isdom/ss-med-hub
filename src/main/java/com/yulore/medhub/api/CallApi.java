package com.yulore.medhub.api;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@FeignClient(value = "${call.srv}")
@ConditionalOnProperty(prefix = "call", name = "srv")
public interface CallApi {
    @Builder
    @Data
    @ToString
    class ApplySessionRequest {
        private String tid;
        private String uuid;
    }

    @RequestMapping(value = "${call.api.apply_session}", method = RequestMethod.POST)
    ApiResponse<ApplySessionVO> apply_session(@RequestBody ApplySessionRequest request);

    @Builder
    @Data
    @ToString
    class MockAnswerRequest {
        private String sessionId;
        private String uuid;
        private String tid;
        private long answerTime;
    }
    @RequestMapping(value = "${call.api.mock_answer}", method = RequestMethod.POST)
    ApiResponse<UserAnswerVO> mock_answer(@RequestBody MockAnswerRequest request);

    @Builder
    @Data
    @ToString
    class UserAnswerRequest {
        private String sessionId;
        private String kid;
        private String tid;
        private String realName;
        private String genderStr;
        private String aesMobile;
        private long answerTime;
    }
    @RequestMapping(value = "${call.api.user_answer}", method = RequestMethod.POST)
    ApiResponse<UserAnswerVO> user_answer(@RequestBody UserAnswerRequest request);
}
