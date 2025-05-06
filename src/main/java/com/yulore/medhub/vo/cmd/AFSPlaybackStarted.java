package com.yulore.medhub.vo.cmd;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.yulore.medhub.vo.WSCommandVO;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;

@Data
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class AFSPlaybackStarted {
    public static final TypeReference<WSCommandVO<AFSPlaybackStarted>> TYPE = new TypeReference<>() {};

    public int localIdx;
    public String playback_id;
    public long eventInMss;
    public long startInMss;
}
