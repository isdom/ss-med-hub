package com.yulore.medhub.ws;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

@Builder
@Data
@ToString
public class HubInfo {
    public int wscount;
}
