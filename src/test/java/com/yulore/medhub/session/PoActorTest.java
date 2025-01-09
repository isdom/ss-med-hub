package com.yulore.medhub.session;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class PoActorTest {

    @Test
    void startTranscription() {
        final String text = "你好！我是中华人民共和国公民。";

        assertEquals(2, countChinesePunctuations(text));
    }

    private static int countChinesePunctuations(final String text) {
        int count = 0;
        for (char c : text.toCharArray()) {
            // 判断是否是中文标点
            if (isChinesePunctuation(c)) {
                count++;
            }
        }
        return count;
    }

    private static boolean isChinesePunctuation(final char ch) {
        // 将需要计算的中文标点放入此数组中
        final Character[] punctuations = new Character[]{
                '，', '。', '！', '？', '；', '：', '“', '”'
        };
        return Arrays.stream(punctuations).anyMatch(p -> p == ch);
    }
}