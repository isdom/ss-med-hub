package com.yulore.medhub.vo.cmd;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.yulore.medhub.vo.WSCommandVO;

public class VOFSPlaybackStopped {
    private static final TypeReference<WSCommandVO<VOFSPlaybackStopped>> TYPE = new TypeReference<>() {};
    public static VOFSPlaybackStopped of(final String message) throws JsonProcessingException {
        return WSCommandVO.parse(message, TYPE).payload;
    }

    public String playback_id;
}
