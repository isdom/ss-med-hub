package com.yulore.medhub.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@AllArgsConstructor
@Data
@ToString
public class PayloadFSHangup {
    String uuid;
    String sessionId;
}