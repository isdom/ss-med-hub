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
public class VOFSPlaybackStarted {
    public static final TypeReference<WSCommandVO<VOFSPlaybackStarted>> TYPE = new TypeReference<>() {};
    public static VOFSPlaybackStarted of(final String message) throws JsonProcessingException {
        return WSCommandVO.parse(message, TYPE).payload;
    }

    public String playback_id;
}
