package com.yulore.nlu;

import java.util.Arrays;

public class ClauseSplitterTest {
    public static void main(String[] args) {
        System.out.printf("%s%n", ClauseSplitter.erasePunctuation("你好啊。"));
        System.out.printf("%s%n",
                ClauseSplitter.splitClausesWithPunctuation("你好啊，怎么说？"));
    }
}
