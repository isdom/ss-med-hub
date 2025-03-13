package com.yulore.medhub.vo.cmd;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.yulore.medhub.vo.WSCommandVO;

public class VOSFileRead {
    private static final TypeReference<WSCommandVO<VOSFileRead>> TYPE = new TypeReference<>() {};
    public static VOSFileRead of(final String message) throws JsonProcessingException {
        return WSCommandVO.parse(message, TYPE).payload;
    }

    public int count;
}
