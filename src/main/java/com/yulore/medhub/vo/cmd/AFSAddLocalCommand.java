package com.yulore.medhub.vo.cmd;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.yulore.medhub.vo.WSCommandVO;
import lombok.Data;
import lombok.ToString;

@Data
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class AFSAddLocalCommand {
    public static final TypeReference<WSCommandVO<AFSAddLocalCommand>> TYPE = new TypeReference<>() {};

    public int localIdx;
    public String uuid;
    public String sessionId;
    public String welcome;
    public long answerInMss;
}
