package com.yulore.medhub.stream;

public interface BuildStreamTask {
    public String key();
    byte[] buildStream();
}
