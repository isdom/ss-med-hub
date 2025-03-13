package com.yulore.medhub.vo.cmd;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.yulore.medhub.vo.WSCommandVO;

public class VOFSRecordStarted {
    private static final TypeReference<WSCommandVO<VOFSRecordStarted>> TYPE = new TypeReference<>() {};
    public static VOFSRecordStarted of(final String message) throws JsonProcessingException {
        return WSCommandVO.parse(message, TYPE).payload;
    }

    public String record_start_timestamp;
}
