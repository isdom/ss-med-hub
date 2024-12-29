package com.yulore.medhub.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@AllArgsConstructor
@Data
@ToString
public class PayloadFSStartPlayback {
    String uuid;
    String playback_id;
    String content_id;
    String file;
}
