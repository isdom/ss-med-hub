package com.yulore.medhub.task;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public interface PlayTask {
    boolean pause();
    void resume();
    void stop();
    boolean isPaused();

    ConcurrentMap<String, PlayTask> _tasks = new ConcurrentHashMap<>();
}
