package com.yulore.medhub.vo.cmd;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.yulore.medhub.vo.WSCommandVO;
import lombok.Data;
import lombok.ToString;

@Data
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class VOPCMPlaybackStarted {
    private static final TypeReference<WSCommandVO<VOPCMPlaybackStarted>> TYPE = new TypeReference<>() {};
    public static VOPCMPlaybackStarted of(final String message) throws JsonProcessingException {
        return WSCommandVO.parse(message, TYPE).payload;
    }

    public String playback_id;
    public String content_id;
}
