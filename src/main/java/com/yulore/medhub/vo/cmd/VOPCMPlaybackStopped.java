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
public class VOPCMPlaybackStopped {
    public static final TypeReference<WSCommandVO<VOPCMPlaybackStopped>> TYPE = new TypeReference<>() {};
    public static VOPCMPlaybackStopped of(final String message) throws JsonProcessingException {
        return WSCommandVO.parse(message, TYPE).payload;
    }

    public String playback_id;
    public String content_id;
    public String playback_begin_timestamp;
    public String playback_end_timestamp;   // eg: "1736389958856"
    public String playback_duration;        // eg: "3.4506666666666668"
}
