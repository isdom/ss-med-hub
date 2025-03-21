package com.yulore.medhub.service;

import java.util.concurrent.Future;

public interface CommandExecutor {
    void submit(Runnable task);
}
