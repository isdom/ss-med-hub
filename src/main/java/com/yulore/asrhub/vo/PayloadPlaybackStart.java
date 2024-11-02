package com.yulore.asrhub.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@AllArgsConstructor
@Data
@ToString
public class PayloadPlaybackStart {
    String file;
    int rate;
    int interval;
    int channels;
}
