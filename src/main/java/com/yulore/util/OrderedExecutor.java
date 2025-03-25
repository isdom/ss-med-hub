package com.yulore.util;

public interface OrderedExecutor {
    public void submit(final int idx, final Runnable task);
    public int idx2order(final int idx);
}
