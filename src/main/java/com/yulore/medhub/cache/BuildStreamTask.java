package com.yulore.medhub.cache;

public interface BuildStreamTask {
    public String key();
    byte[] buildStream();
}
