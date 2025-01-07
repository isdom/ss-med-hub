package com.yulore.medhub.task;

public interface PlayTask {
    boolean pause();
    void resume();
    void stop();
    boolean isPaused();
}
