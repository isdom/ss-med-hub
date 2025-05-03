package com.yulore.medhub.service;

import java.nio.ByteBuffer;

public interface ASROperator {
    boolean transmit(final ByteBuffer data);
    void close();
}
