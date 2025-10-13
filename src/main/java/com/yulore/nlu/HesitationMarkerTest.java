package com.yulore.nlu;

import java.util.List;

public class HesitationMarkerTest {
    public static void main(String[] args) {
        HesitationMarkerExtractor extractor = new HesitationMarkerExtractor();

        String[] testCases = {
                "嗯，你好",
                "喂！有什么事",
                "哦...明白了",
                "啊哎呃，这么多",
                "你好嗯，今天怎么样",
                "嗯喂哦！",
                "呃？真的吗",
                "先说你好，嗯再见",
                "嗯",
                "啊？哦！呃。"
        };

        System.out.println("=== 测试语气助词提取 ===");
        for (String text : testCases) {
            List<String> phrases = extractor.extractWithPunctuation(text);
            System.out.printf("'%s' -> %s%n", text, phrases);
        }

        System.out.println("\n=== 测试位置信息提取 ===");
        String sampleText = "嗯，你好啊，喂！今天怎么样？呃，没什么事";
        List<HesitationMarkerExtractor.HesitationPosition> positions = extractor.extractPositions(sampleText);
        for (HesitationMarkerExtractor.HesitationPosition pos : positions) {
            System.out.println(pos);
        }

        System.out.println("\n=== 测试语气助词移除 ===");
        String cleaned = extractor.removeHesitationPhrases(sampleText);
        System.out.printf("原始: '%s'%n", sampleText);
        System.out.printf("清理后: '%s'%n", cleaned);
    }
}