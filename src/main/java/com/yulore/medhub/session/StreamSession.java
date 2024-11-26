package com.yulore.medhub.session;

import com.yulore.util.ByteArrayListInputStream;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

@ToString
@Slf4j
public class StreamSession {
    public StreamSession(final String path, final String sessionId, final String contentId, final String playIdx) {
        _path = path;
        _sessionId = sessionId;
        _contentId = contentId;
        _playIdx = playIdx;
    }
//    public StreamSession(final InputStream is, final int length) {
//        _is = is;
//        _length = length;
//    }

    public void lock() {
        _lock.lock();
        log.info("lock session: {}", _lock);
    }

    public void unlock() {
        log.info("unlock session: {}", _lock);
        _lock.unlock();
    }

    public boolean streaming() {
        try {
            _lock.lock();
            return _streaming;
        } finally {
            _lock.unlock();
        }
    }

    public InputStream genInputStream() {
        final InputStream is = new ByteArrayListInputStream(_bufs);
        try {
            is.skip(_pos);
        } catch (IOException ignored) {
        }
        return is;
    }

    public boolean needMoreData(final int count4read) {
        try {
            _lock.lock();
            return _streaming && _pos + count4read > _length;
        } finally {
            _lock.unlock();
        }
    }

    public int length() {
        try {
            _lock.lock();
            return _streaming ? Integer.MAX_VALUE : _length;
        } finally {
            _lock.unlock();
        }
    }

    public int tell() {
        try {
            _lock.lock();
            return _pos;
        } finally {
            _lock.unlock();
        }
    }

    public int seekFromStart(int pos) {
        try {
            _lock.lock();
            _pos = pos;
            return _pos;
        } finally {
            _lock.unlock();
        }
    }

    public void onDataChange(final Function<StreamSession, Boolean> onDataChanged) {
        _onDataChanged = onDataChanged;
    }

    public void appendData(final byte[] bytes) {
        try {
            _lock.lock();
            _bufs.add(bytes);
            _length += bytes.length;
            callOnDataChanged();
        } finally {
            _lock.unlock();
        }
    }

    public void appendCompleted() {
        try {
            _lock.lock();
            _streaming = false;
            callOnDataChanged();
        } finally {
            _lock.unlock();
        }
    }

    private void callOnDataChanged() {
        if (_onDataChanged != null) {
            if (_onDataChanged.apply(this)) {
                _onDataChanged = null;
            }
        }
    }

    final private String _path;
    final private String _sessionId;
    final private String _contentId;
    final private String _playIdx;

    private int _length = 0;
    private int _pos = 0;
    private boolean _streaming = true;

    final List<byte[]> _bufs = new ArrayList<>();
    private final Lock _lock = new ReentrantLock();
    private Function<StreamSession, Boolean> _onDataChanged = null;
}
