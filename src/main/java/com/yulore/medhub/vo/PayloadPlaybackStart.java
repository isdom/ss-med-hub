package com.yulore.medhub.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@AllArgsConstructor
@Data
@ToString
public class PayloadPlaybackStart {
    int id;
    String file;
    int rate;
    int interval;
    int channels;
}
