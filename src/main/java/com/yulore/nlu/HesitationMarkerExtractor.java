package com.yulore.nlu;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HesitationMarkerExtractor {
    public static final Pattern punctuationPattern = Pattern.compile("^[、；。，？！]+");
    private final Pattern strictPattern;

    public HesitationMarkerExtractor() {
        // 严格匹配语气助词的正则表达式
        this.strictPattern = Pattern.compile("(?<=^|[、；。，？！])([嗯喂哦啊哎呃]+)(?=[、；。，？！]|$)");
    }

    /**
     * 提取语气助词及其后的标点
     * @param text 输入文本
     * @return 语气助词短语列表
     */
    public List<String> extractWithPunctuation(String text) {
        final List<String> results = new ArrayList<>();
        Matcher matcher = strictPattern.matcher(text);

        while (matcher.find()) {
            int startPos = matcher.start(1);
            String hesitation = matcher.group(1);

            // 检查并添加后面的标点
            int hesitationEnd = startPos + hesitation.length();
            if (hesitationEnd < text.length()) {
                final String remaining = text.substring(hesitationEnd);
                final Matcher punctMatcher = punctuationPattern.matcher(remaining);

                if (punctMatcher.find()) {
                    results.add(hesitation + punctMatcher.group(0));
                } else {
                    // 后面没有标点，但可能是字符串结束
                    results.add(hesitation);
                }
            } else {
                // 字符串结束
                results.add(hesitation);
            }
        }

        return results;
    }

    /**
     * 返回语气助词的位置信息
     * @param text 输入文本
     * @return 位置信息列表
     */
    public List<HesitationPosition> extractPositions(String text) {
        List<HesitationPosition> positions = new ArrayList<>();
        Matcher matcher = strictPattern.matcher(text);

        while (matcher.find()) {
            String hesitation = matcher.group(1);
            int start = matcher.start(1);
            int end = matcher.end(1);

            // 检查后面的标点
            String punctuation = "";
            if (end < text.length()) {
                String remaining = text.substring(end);
                Matcher punctMatcher = punctuationPattern.matcher(remaining);
                if (punctMatcher.find()) {
                    punctuation = punctMatcher.group(0);
                    end += punctuation.length(); // 更新结束位置包含标点
                }
            }

            positions.add(new HesitationPosition(hesitation, start, end, hesitation + punctuation));
        }

        return positions;
    }

    /**
     * 移除文本中的语气助词短语
     * @param text 输入文本
     * @return 清理后的文本
     */
    public String removeHesitationPhrases(String text) {
        List<HesitationPosition> positions = extractPositions(text);
        if (positions.isEmpty()) {
            return text;
        }

        // 从后往前移除，避免位置偏移
        StringBuilder result = new StringBuilder(text);
        for (int i = positions.size() - 1; i >= 0; i--) {
            HesitationPosition pos = positions.get(i);
            result.replace(pos.start, pos.end, "");
        }

        return result.toString().trim();
    }

    /**
     * 内部类：存储语气助词位置信息
     */
    public static class HesitationPosition {
        public final String hesitation;
        public final int start;
        public final int end;
        public final String fullMatch;

        public HesitationPosition(String hesitation, int start, int end, String fullMatch) {
            this.hesitation = hesitation;
            this.start = start;
            this.end = end;
            this.fullMatch = fullMatch;
        }

        @Override
        public String toString() {
            return String.format("HesitationPosition{text='%s', start=%d, end=%d, fullMatch='%s'}",
                    hesitation, start, end, fullMatch);
        }
    }
}