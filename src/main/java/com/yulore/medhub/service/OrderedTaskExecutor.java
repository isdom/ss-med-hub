package com.yulore.medhub.service;

public interface OrderedTaskExecutor {
    public void submit(final int ownerIdx, final Runnable task);
}
