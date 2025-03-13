package com.yulore.medhub.vo.cmd;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.yulore.medhub.vo.WSCommandVO;

public class VOUserAnswer {
    private static final TypeReference<WSCommandVO<VOUserAnswer>> TYPE = new TypeReference<>() {};
    public static VOUserAnswer of(final String message) throws JsonProcessingException {
        return WSCommandVO.parse(message, TYPE).payload;
    }

    public String kid;
    public String tid;
    public String realName;
    public String aesMobile;
    public String genderStr;
}
