package com.yulore.bst;

import java.util.function.Consumer;

public interface BuildStreamTask {
    String key();
    void buildStream(Consumer<byte[]> onPart, Consumer<Boolean> onCompleted);
}
