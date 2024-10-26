package com.yulore.asrproxy.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@AllArgsConstructor
@Data
@ToString
public class PayloadTranscriptionResultChanged {
    int index;
    int time;
    String result;
}
