package com.yulore.medhub.vo.cmd;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.yulore.medhub.vo.WSCommandVO;
import lombok.Data;
import lombok.ToString;

@Data
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class AFSRemoveLocalCommand {
    public static final TypeReference<WSCommandVO<AFSRemoveLocalCommand>> TYPE = new TypeReference<>() {};

    public int localIdx;
}
