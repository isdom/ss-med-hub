package com.yulore.medhub.service;

import java.util.concurrent.Future;

public interface CommandExecutor {
    Future<?> submit(Runnable task);
}
