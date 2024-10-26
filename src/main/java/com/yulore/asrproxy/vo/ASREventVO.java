package com.yulore.asrproxy.vo;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class ASREventVO<PAYLOAD> {
    @Data
    @ToString
    public static class Header {
        String name;
    }
    Header header;
    PAYLOAD payload;
}
