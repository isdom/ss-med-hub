package com.yulore.medhub.task;

import com.yulore.util.ByteArrayListInputStream;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

@RequiredArgsConstructor
@ToString(of={"_taskId", "_path", "_started", "_stopped", "_paused", "_completed"})
@Slf4j
public class PlayStreamPCMTask2 implements PlayTask {
    private final String _taskId;
    final String _sessionId;
    final String _path;
    final ScheduledExecutorService _executor;
    final SampleInfo _sampleInfo;
    private final Consumer<Long> _onStartSend;
    private final Consumer<Long> _onStopSend;
    private final Consumer<byte[]> _doSendData;
    private final AtomicLong _startSendTimestamp = new AtomicLong(0);

    private final Consumer<PlayStreamPCMTask2> _onEnd;
    private final AtomicBoolean _completed = new AtomicBoolean(false);

    int _interval_bytes;
    final AtomicReference<ScheduledFuture<?>> _next = new AtomicReference<>(null);
    private long _startTimestamp;

    final AtomicBoolean _started = new AtomicBoolean(false);
    final AtomicBoolean _stopped = new AtomicBoolean(false);
    final AtomicBoolean _paused = new AtomicBoolean(false);
    final AtomicBoolean _stopEventSended = new AtomicBoolean(false);

    private boolean _streaming = true;
    private int _pos = 0;
    private int _length = 0;
    final List<byte[]> _bufs = new ArrayList<>();
    private final Lock _lock = new ReentrantLock();

    private final AtomicInteger _sendDataCounter = new AtomicInteger(0);
    private final AtomicLong _sendDataLastTimestamp = new AtomicLong(0);
    private final AtomicReference<StringBuilder> _sendDataIntervalLog = new AtomicReference<>(new StringBuilder());

    public String taskId() {
        return _taskId;
    }

    public String path() {
        return _path;
    }

    public boolean isPaused() {
        return _paused.get();
    }

    public boolean isCompleted() {
        return _completed.get();
    }

    public void appendData(final byte[] bytes) {
        try {
            _lock.lock();
            _bufs.add(bytes);
            _length += bytes.length;
            if (!_started.get()) {
                start();
            }
        } finally {
            _lock.unlock();
        }
    }

    public void appendCompleted() {
        try {
            _lock.lock();
            _streaming = false;
        } finally {
            _lock.unlock();
        }
    }

    public void start() {
        try {
            _lock.lock();
            if (_stopped.get()) {
                // maybe call stop() before call start()
                log.warn("[{}]: pcm task ({}) has stopped, can't start again", _sessionId, this);
                return;
            }
            _interval_bytes = _sampleInfo.bytesPerInterval();
            if (_started.compareAndSet(false, true)) {
                log.info("[{}]: pcm task ({}) start to playback", _sessionId, this);
                _startTimestamp = System.currentTimeMillis();
                _sendDataLastTimestamp.set(_startTimestamp);
                playAndSchedule(1 );
            } else {
                log.warn("[{}]: pcm task ({}) started, ignore multi-call start()", _sessionId, this);
            }
        } finally {
            _lock.unlock();
        }
    }

    final static int MAX_PACKAGE_NUM_ONCE = 50;

    private void playAndSchedule(final int intervalCount) {
        final int remaining = _length - _pos;
        final int packageNum = Math.min(Math.max(remaining / _interval_bytes, 1), MAX_PACKAGE_NUM_ONCE);
        final int batchSize = _interval_bytes * packageNum;

        ScheduledFuture<?> next = null;
        try {
            _lock.lock();
            if (_stopped.get()) {
                // ignore if stopped flag set
                log.warn("[{}]: pcm task ({}) has stopped, abort playAndSchedule", _sessionId, this);
                return;
            }
            if (_pos + batchSize > _length && _streaming) {
                // need more data
                final long delay = 100; // 100 ms // _startTimestamp + (long) _sampleInfo.interval() * intervalCount - System.currentTimeMillis();
                log.warn("[{}]: pcm task ({}) need_more_data (remain: {} < batchSize: {}), delay_playback_to_next",
                        _sessionId, this, remaining, batchSize);
                next = _executor.schedule(() -> playAndSchedule(intervalCount + 1), delay, TimeUnit.MILLISECONDS);
            } else {
                final byte[] bytes = new byte[batchSize];
                final int read_size = fillIntervalData(bytes);
                if (read_size >= _interval_bytes) {
                    fireStartSendOnce();
                    if (read_size == batchSize) {
                        _doSendData.accept(bytes);
                    } else {
                        final byte[] send_bytes = new byte[read_size];
                        System.arraycopy(bytes, 0, send_bytes, 0, read_size);
                        _doSendData.accept(send_bytes);
                    }
                    final int send_count = _sendDataCounter.incrementAndGet();
                    final long now = System.currentTimeMillis();
                    final long delay = 1; // 1ms // _startTimestamp + (long) _sampleInfo.interval() * intervalCount - now;
                    next = _executor.schedule(() -> playAndSchedule(intervalCount + 1), delay, TimeUnit.MILLISECONDS);
                    final int interval_in_ms = (int)(now - _sendDataLastTimestamp.get());
                    _sendDataLastTimestamp.set(now);
                    if (send_count % 50 == 0) {
                        outputSendDataIntervalLogsAndReset();
                    }
                    _sendDataIntervalLog.get().append(interval_in_ms);
                    _sendDataIntervalLog.get().append(' ');
                } else {
                    _stopped.compareAndSet(false, true);
                    _completed.compareAndSet(false, true);
                    safeSendPlaybackStopEvent();
                    log.info("[{}]: finish ({}) playback by {} send action", _sessionId, this, intervalCount);
                }
            }
        } catch (IOException ex) {
            log.warn("[{}]: pcm task ({}) schedule failed, detail: {}", _sessionId, this, ex.toString());
            throw new RuntimeException(ex);
        } finally {
            _next.set(next);
            _lock.unlock();
        }
    }

    private void outputSendDataIntervalLogsAndReset() {
        log.info("[{}]: {} pcm_send_data_interval: {}", _sessionId, _taskId, _sendDataIntervalLog.get());
        _sendDataIntervalLog.set(new StringBuilder());
    }

    private int fillIntervalData(final byte[] bytes) throws IOException {
        //if (_paused.get()) {
            // playback in paused state, so send silent data
        //    return bytes.length;
        //} else {
            try (final InputStream is = new ByteArrayListInputStream(_bufs)) {
                is.skip(_pos);
                final int read_size = is.read(bytes);
                _pos += read_size;
                return read_size;
            }
        //}
    }

    private void fireStartSendOnce() {
        if (_startSendTimestamp.compareAndSet(0, 1)) {
            _startSendTimestamp.set(System.currentTimeMillis());
            _onStartSend.accept(_startSendTimestamp.get());
        }
    }

    public boolean pause() {
        if (_stopped.get()) {
            // ignore if stopped flag set
            log.warn("[{}]: pcm task ({}) has stopped, ignore pause request", _sessionId, this);
            return false;
        }
        if (_paused.compareAndSet(false, true)) {
            log.info("[{}]: pcm task ({}) change to paused state", _sessionId, this);
            return true;
        } else {
            log.info("[{}]: pcm task ({}) paused already, ignore pause request", _sessionId, this);
            return false;
        }
    }

    public void resume() {
        if (_stopped.get()) {
            // ignore if stopped flag set
            log.warn("[{}]: pcm task ({}) has stopped, ignore resume request", _sessionId, this);
            return;
        }
        if (_paused.compareAndSet(true, false)) {
            log.info("[{}]: pcm task ({}) resume to playback", _sessionId, this);
        } else {
            log.info("[{}]: pcm task ({}) !NOT! paused, ignore resume request", _sessionId, this);
        }
    }

    public void stop() {
        try {
            _lock.lock();
            if (_stopped.compareAndSet(false, true)) {
                if (_started.get()) {
                    final ScheduledFuture<?> current = _next.getAndSet(null);
                    if (null != current) {
                        current.cancel(false);
                    }
                    safeSendPlaybackStopEvent();
                }
            }
        } finally {
            _lock.unlock();
        }
    }

    private void safeSendPlaybackStopEvent() {
        if (_stopEventSended.compareAndSet(false, true)) {
            _startSendTimestamp.set(0);
            _onStopSend.accept(System.currentTimeMillis());
            _onEnd.accept(this);
            outputSendDataIntervalLogsAndReset();
        }
    }
}
