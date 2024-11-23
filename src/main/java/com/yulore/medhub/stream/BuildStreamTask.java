package com.yulore.medhub.stream;

import java.util.function.Consumer;

public interface BuildStreamTask {
    public String key();
    void buildStream(Consumer<byte[]> onPart, Consumer<Boolean> onCompleted);
}
