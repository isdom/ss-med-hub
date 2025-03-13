package com.yulore.medhub.vo.cmd;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.yulore.medhub.vo.WSCommandVO;

public class VOSFileSeek {
    private static final TypeReference<WSCommandVO<VOSFileSeek>> TYPE = new TypeReference<>() {};
    public static VOSFileSeek of(final String message) throws JsonProcessingException {
        return WSCommandVO.parse(message, TYPE).payload;
    }

    public int offset;
    public int whence;
}
