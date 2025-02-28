package com.yulore.medhub.vo;

import lombok.Data;
import lombok.ToString;

import java.util.Map;

@Data
@ToString
public class WSCommandVO {
    Map<String, String> header;
    Map<String, String> payload;
}
