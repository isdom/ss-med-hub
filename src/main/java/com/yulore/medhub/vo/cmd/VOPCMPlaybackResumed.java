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
public class VOPCMPlaybackResumed {
    private static final TypeReference<WSCommandVO<VOPCMPlaybackResumed>> TYPE = new TypeReference<>() {};
    public static VOPCMPlaybackResumed of(final String message) throws JsonProcessingException {
        return WSCommandVO.parse(message, TYPE).payload;
    }

    public String playback_id;
    public String content_id;
    public String playback_duration; //"4.410666666666666"
}
