package com.yulore.medhub.api;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

@FeignClient(value = "${CALL_PRPOVIDER}")
public interface CallApi {
    @RequestMapping(value = "${script.api.apply_session}", method = RequestMethod.GET)
    ApiResponse<ApplySessionVO> apply_session(
            @RequestParam("kid")        String kid,
            @RequestParam("tid")        String tid,
            @RequestParam("realName")   String realName,
            @RequestParam("lastName")   String lastName,
            @RequestParam("genderStr")  String genderStr,
            @RequestParam("aesMobile")  String aesMobile,
            @RequestParam("answerTime") long answerTimeInMs
            );
}
