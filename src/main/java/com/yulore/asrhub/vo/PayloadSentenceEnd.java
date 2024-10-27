package com.yulore.asrhub.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@AllArgsConstructor
@Data
@ToString
public class PayloadSentenceEnd {
    int index;
    int time;
    int begin_time;
    String result;
    double confidence;
}
