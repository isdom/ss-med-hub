package com.yulore.asrhub.vo;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class HubEventVO<PAYLOAD> {
    @Data
    @ToString
    public static class Header {
        String name;
    }
    Header header;
    PAYLOAD payload;
}
