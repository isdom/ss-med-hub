package com.yulore.medhub.api;

import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Map;

@EqualsAndHashCode(callSuper = true)
@Data
@ToString(callSuper = true)
public class ApplySessionVO extends AIReplyVO {
    @SerializedName("sessionId")
    private String sessionId;
}
