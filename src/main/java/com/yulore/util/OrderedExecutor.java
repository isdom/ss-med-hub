package com.yulore.util;

public interface OrderedExecutor {
    void submit(final int idx, final Runnable task);
    int idx2order(final int idx);
}
