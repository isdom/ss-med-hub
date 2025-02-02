package com.yulore.medhub.session;

import com.yulore.medhub.stream.VarsUtil;
import com.yulore.util.ByteArrayListInputStream;
import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
@ToString(of={"_sessionId", "_contentId", "_playIdx"})
public class StreamSession {
    @AllArgsConstructor
    static public class EventContext {
        public String name;
        public Object payload;
        public long start;
        public StreamSession session;
    }

    @AllArgsConstructor
    static public class DataContext {
        public ByteBuffer data;
        public long start;
        public StreamSession session;
    }

    @AllArgsConstructor
    static public class UploadToOSSContext {
        public String bucketName;
        public String objectName;
        public InputStream content;
    }

    public StreamSession(final boolean isWrite,
                         final Consumer<EventContext> doSendEvent,
                         final Consumer<DataContext> doSendData,
                         final Consumer<UploadToOSSContext> doUpload,
                         final String path,
                         final String sessionId,
                         final String contentId,
                         final String playIdx) {
        _isWrite = isWrite;
        _doSendEvent = doSendEvent;
        _doSendData = doSendData;
        _doUpload = doUpload;
        _path = path;
        _sessionId = sessionId;
        _contentId = contentId;
        _playIdx = playIdx;
        if (_isWrite) {
            _streaming = false;
        }
    }

    public void close() {
        if (_isWrite) {
            // eg: rms://{uuid={uuid},bucket=ylhz-aicall,url=ws://172.18.86.131:6789/record}<objectName>
            final int braceBegin = _path.indexOf('{');
            if (braceBegin == -1) {
                log.warn("{} missing vars, ignore", _path);
                return;
            }
            final int braceEnd = _path.indexOf('}');
            if (braceEnd == -1) {
                log.warn("{} missing vars, ignore", _path);
                return;
            }
            final String vars = _path.substring(braceBegin + 1, braceEnd);

            final String bucketName = VarsUtil.extractValue(vars, "bucket");
            if (null == bucketName) {
                log.warn("{} missing bucket field, ignore", _path);
                return;
            }

            final String objectName = _path.substring(braceEnd + 1);
            _doUpload.accept(new UploadToOSSContext(bucketName, objectName, new ByteArrayListInputStream(_bufs)));
        }
    }

    public String sessionId () {
        return _sessionId;
    }

    public void sendEvent(final long startInMs, final String eventName, final Object payload) {
        _doSendEvent.accept(new EventContext(eventName, payload, startInMs, this));
    }

    public void sendData(final long startInMs, final ByteBuffer data) {
        _doSendData.accept(new DataContext(data, startInMs, this));
    }

    public void lock() {
        _lock.lock();
        // log.info("lock session: {}", _lock);
    }

    public void unlock() {
        // log.info("unlock session: {}", _lock);
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

    public int seekFromStart(final int pos) {
        try {
            _lock.lock();
            _pos = pos;
            if (_isWrite && _pos > _length) {
                _pos = _length;
            }
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

    public int writeToStream(final ByteBuffer bytes) {
        final byte[] srcBytes = new byte[bytes.remaining()];
        bytes.get(srcBytes, 0, srcBytes.length);

        try {
            _lock.lock();
            if (_pos >= _length) {
                log.info("[{}]: writeToStream for pos: {} >= length: {}, append data {} bytes directly",
                        _sessionId, _pos, _length, srcBytes.length);
                // append data on the end
                doAppendBytes(srcBytes);
                log.info("[{}]: writeToStream => doAppendBytes: pos: {}/length: {}", _sessionId, _pos, _length);
                return srcBytes.length;
            }

            log.info("[{}]: writeToStream for pos: {} < length: {}, need rewrite exist bufs {} bytes",
                    _sessionId, _pos, _length, srcBytes.length);
            // copy some data to exist bytesList
            int posInBuf = 0, idxOfBuf = 0, off = 0;
            byte[] curBuf;
            for (; idxOfBuf < _bufs.size(); idxOfBuf++) {
                curBuf = _bufs.get(idxOfBuf);
                if (off + curBuf.length > _pos) {
                    posInBuf = _pos - off;
                    break;
                }
                off += curBuf.length;
            }
            log.info("[{}]: writeToStream re-write: idxOfBuf:{}/posInBuf:{}", _sessionId, idxOfBuf, posInBuf);

            final int leftToWrite = writeToExistBufs(srcBytes, idxOfBuf, posInBuf);

            log.info("[{}]: writeToStream => writeToExistBufs: leftToWrite: {}", _sessionId, leftToWrite);

            // write to end of stream
            if (leftToWrite > 0) {
                // and has data to write, so append at the end of stream
                final byte[] leftBytes = new byte[leftToWrite];
                System.arraycopy(srcBytes, srcBytes.length - leftToWrite, leftBytes, 0, leftToWrite);
                doAppendBytes(leftBytes);
                log.info("[{}]: writeToStream => doAppendBytes: pos: {}/length: {}", _sessionId, _pos, _length);
                return srcBytes.length;
            }
        } finally {
            _lock.unlock();
        }
        return srcBytes.length;
    }

    private void doAppendBytes(final byte[] bytes) {
        _bufs.add(bytes);
        _pos += bytes.length;
        _length += bytes.length;
    }

    private int writeToExistBufs(final byte[] bytesArray, int idxOfBuf, int posInBuf) {
        int leftLen = bytesArray.length, writeSize = leftLen;
        while (idxOfBuf < _bufs.size()) {
            final byte[] curBuf = _bufs.get(idxOfBuf);

            if (posInBuf < curBuf.length) {
                if (posInBuf + writeSize > curBuf.length) {
                    writeSize = curBuf.length - posInBuf;
                }

                System.arraycopy(bytesArray, bytesArray.length - leftLen, curBuf, posInBuf, writeSize);
                // off += readSize;
                posInBuf += writeSize;
                _pos += writeSize;
                leftLen -= writeSize;
                writeSize = leftLen;
                if (leftLen == 0) {
                    // data for write has been full-written
                    break;
                }
            }
            else {
                idxOfBuf++;
                posInBuf = 0;
            }
        }
        return leftLen;
    }

    final private boolean _isWrite;
    final private String _path;
    final private String _sessionId;
    final private String _contentId;
    final private String _playIdx;
    final private Consumer<EventContext> _doSendEvent;
    final private Consumer<DataContext> _doSendData;
    final private Consumer<UploadToOSSContext> _doUpload;

    private int _length = 0;
    private int _pos = 0;
    private boolean _streaming = true;

    final List<byte[]> _bufs = new ArrayList<>();
    private final Lock _lock = new ReentrantLock();
    private Function<StreamSession, Boolean> _onDataChanged = null;
}
