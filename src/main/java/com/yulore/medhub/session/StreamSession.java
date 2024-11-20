package com.yulore.medhub.session;

import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;

@Data
@ToString
@Slf4j
public class StreamSession {
    public StreamSession(final InputStream is, final int length) {
        _is = is;
        _length = length;
    }

    final private InputStream _is;
    final private int _length;
}
