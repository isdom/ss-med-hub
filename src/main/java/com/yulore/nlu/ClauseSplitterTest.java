package com.yulore.nlu;

public class ClauseSplitterTest {
    public static void main(String[] args) {
        System.out.printf("%s%n", ClauseSplitter.erasePunctuation("你好啊。"));
        System.out.printf("%s%n",
                ClauseSplitter.splitClausesWithPunctuation("你好啊，怎么说？"));
        System.out.printf("%s%n",
                ClauseSplitter.splitClauses("你好啊，怎么说？"));
    }
}
