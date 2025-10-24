package com.yulore.nlu;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class NluUtilTest {
    public static void main(String[] args) {
        final Map<String, Integer> cu2in = Map.of("哪位", 167, "您说", 157);

        String sampleText = "我是的，哪位？您说你说";
        System.out.printf("原始: '%s'%n", sampleText);

        final var result = NluUtil.extractAndRemoveClause(cu2in, sampleText);
        System.out.printf("清理后: '%s' => ins: %s%n", result.cleanedText(),
                Arrays.stream(result.ins()).map(String::valueOf).collect(Collectors.joining(",")));

        //RedisUtil.release_all();
    }
}
