package com.yulore.util;

public interface OrderedTaskExecutor {
    public void submit(final int ownerIdx, final Runnable task);
}
