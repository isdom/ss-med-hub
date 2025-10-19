package com.yulore.znc.vo;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@RequiredArgsConstructor
@Getter
@ToString
public class RemovePhraseResult {
    private final String orgId;
    private final String cleanedText;
    private final String cleanedId;
    private final Integer[] ins;
}
