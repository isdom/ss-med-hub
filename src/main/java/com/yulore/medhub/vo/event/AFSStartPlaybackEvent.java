package com.yulore.medhub.vo.event;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Builder
@Getter
@ToString
public class AFSStartPlaybackEvent {
    final int localIdx;
    final String playback_id;
    final String content_id;
    final String file;
}
