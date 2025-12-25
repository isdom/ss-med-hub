package com.yulore.medhub.api;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

@Data
@Builder
@ToString
public class Intent2ReplyRequest {
    private String  sessionId;
    private Integer speechIdx;
    private String  speechText;
    private String  traceId;
    private String  intent;
    private Integer[] sysIntents;
    private Integer isSpeaking;
    private Long    speakingContentId;
    private Integer speakingDurationMs;
    private Long    idleTime;
}
