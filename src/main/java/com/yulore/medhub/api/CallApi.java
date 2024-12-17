package com.yulore.medhub.api;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@FeignClient(value = "${CALL_PROVIDER}")
public interface CallApi {
    @Builder
    @Data
    @ToString
    class ApplySessionRequest {
        private String kid;
        private String tid;
        private String realName;
        private String genderStr;
        private String aesMobile;
        private long answerTime;
    }

    @RequestMapping(value = "${call.api.apply_session}", method = RequestMethod.POST)
    ApiResponse<ApplySessionVO> apply_session(@RequestBody ApplySessionRequest request);
}
