package com.yulore.medhub.vo.cmd;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.yulore.medhub.vo.WSCommandVO;

public class VOSOpenStream {
    private static final TypeReference<WSCommandVO<VOSOpenStream>> TYPE = new TypeReference<>() {};
    public static VOSOpenStream of(final String message) throws JsonProcessingException {
        return WSCommandVO.parse(message, TYPE).payload;
    }

    public String path;
    public boolean is_write;
    public String session_id;
    public String content_id;
    public String playback_idx;
}
