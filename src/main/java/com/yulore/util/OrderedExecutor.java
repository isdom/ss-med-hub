package com.yulore.util;

public interface OrderedExecutor {
    public void submit(final int ownerIdx, final Runnable task);
}
