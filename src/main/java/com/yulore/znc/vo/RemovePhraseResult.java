package com.yulore.znc.vo;

public record RemovePhraseResult(String orgId, String cleanedText, String cleanedId, Integer[] ins) {
}
