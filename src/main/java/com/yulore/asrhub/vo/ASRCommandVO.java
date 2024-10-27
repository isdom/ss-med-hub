package com.yulore.asrhub.vo;

import lombok.Data;
import lombok.ToString;

import java.util.Map;

@Data
@ToString
public class ASRCommandVO {
    Map<String, String> header;
    Map<String, String> payload;
}
