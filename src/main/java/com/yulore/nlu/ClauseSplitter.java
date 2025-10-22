package com.yulore.nlu;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class ClauseSplitter {
    static final Pattern cp_pattern = Pattern.compile(
            // 不带标点捕获
            "([^，。？！；：、]+)[，。？！；：、]"
    );

    static final Pattern cp_pattern_with_punctuation = Pattern.compile(
            // 带标点捕获，中文常见分句标点（全角）
            "[^，。？！；：、]*[，。？！；：、]"
    );

    public static List<String> splitClauses(final String sentence) {
        final var matcher = cp_pattern.matcher(sentence);
        final List<String> clauses = new ArrayList<>();
        int lastEnd = 0; // 记录上一次匹配结束的位置

        while (matcher.find()) {
            clauses.add(matcher.group(1));
            lastEnd = matcher.end(); // 安全：在 find() 成功后调用
        }

        // 处理尾部未匹配的部分（例如句子末尾没有标点）
        if (lastEnd < sentence.length()) {
            String trailing = sentence.substring(lastEnd).trim();
            if (!trailing.isEmpty()) {
                clauses.add(trailing);
            }
        }
        return clauses;
    }

    public static List<String> splitClausesWithPunctuation(final String sentence) {
        final var matcher = cp_pattern_with_punctuation.matcher(sentence);
        final List<String> clauses = new ArrayList<>();
        int lastEnd = 0; // 记录上一次匹配结束的位置

        while (matcher.find()) {
            clauses.add(matcher.group());
            lastEnd = matcher.end(); // 安全：在 find() 成功后调用
        }

        // 处理尾部未匹配的部分（例如句子末尾没有标点）
        if (lastEnd < sentence.length()) {
            String trailing = sentence.substring(lastEnd);
            clauses.add(trailing);
        }
        return clauses;
    }

    public static String erasePunctuation(final String clause) {
        return clause.replaceAll("[，。？！；：、]", "");
    }
}
