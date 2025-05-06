package com.yulore.medhub.vo.event;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Builder
@Getter
@ToString
public class AFSHangupEvent {
    final int localIdx;
    final long hangupInMs;
}
