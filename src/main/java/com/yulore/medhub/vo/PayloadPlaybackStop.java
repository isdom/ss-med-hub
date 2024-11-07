package com.yulore.medhub.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@AllArgsConstructor
@Data
@ToString
public class PayloadPlaybackStop {
    int id;
    String file;
    int samples;
    boolean completed;
}
