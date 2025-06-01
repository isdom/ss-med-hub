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
public class AFSRecordStarted {
    public static final TypeReference<WSCommandVO<AFSRecordStarted>> TYPE = new TypeReference<>() {};

    public int localIdx;
    public long recordStartInMss;
    public long fbwInMss;
    public int fbwBytes;
}
