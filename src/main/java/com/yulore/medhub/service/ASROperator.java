package com.yulore.medhub.service;

import java.nio.ByteBuffer;

public interface ASROperator {
    boolean transmit(final byte[] data);
    void close();
}
