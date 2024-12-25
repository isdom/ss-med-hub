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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

@RequiredArgsConstructor
@ToString(of={"_taskId", "_path", "_started", "_stopped", "_paused", "_completed"})
@Slf4j
public class PlayStreamPCMTask {
    private final String _taskId = UUID.randomUUID().toString();
    final String _sessionId;
    final String _path;
    final ScheduledExecutorService _executor;
    final SampleInfo _sampleInfo;
    private final Consumer<Long> _onStartSend;
    private final Consumer<Long> _onStopSend;
    private final Consumer<byte[]> _doSendData;
    private final AtomicLong _startSendTimestamp = new AtomicLong(0);

    private final Consumer<PlayStreamPCMTask> _onEnd;
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
                // HubEventVO.sendEvent(_webSocket, "PlaybackStart", new PayloadPlaybackStart(0,"pcm", _sampleInfo.sampleRate, _sampleInfo.interval, _sampleInfo.channels));
                _startTimestamp = System.currentTimeMillis();
                playAndSchedule(1 );
            } else {
                log.warn("[{}]: pcm task ({}) started, ignore multi-call start()", _sessionId, this);
            }
        } finally {
            _lock.unlock();
        }
    }

    private void playAndSchedule(final int intervalCount) {
        ScheduledFuture<?> next = null;
        try {
            _lock.lock();
            if (_stopped.get()) {
                // ignore if stopped flag set
                log.warn("[{}]: pcm task ({}) has stopped, abort playAndSchedule", _sessionId, this);
                return;
            }
            if (_pos + _interval_bytes > _length && _streaming) {
                // need more data
                final long delay = _startTimestamp + (long) _sampleInfo.interval() * intervalCount - System.currentTimeMillis();
                next = _executor.schedule(() -> playAndSchedule(intervalCount + 1), delay, TimeUnit.MILLISECONDS);
                log.warn("[{}]: pcm task ({}) need_more_data, delay_playback_to_next", _sessionId, this);
            } else {
                final byte[] bytes = new byte[_interval_bytes];
                final int read_size = fillIntervalData(bytes);
                if (read_size == _interval_bytes) {
                    fireStartSendOnce();
                    _doSendData.accept(bytes);
                    final long delay = _startTimestamp + (long) _sampleInfo.interval() * intervalCount - System.currentTimeMillis();
                    next = _executor.schedule(() -> playAndSchedule(intervalCount + 1), delay, TimeUnit.MILLISECONDS);
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

    private int fillIntervalData(final byte[] bytes) throws IOException {
        if (_paused.get()) {
            // playback in paused state, so send silent data
            return bytes.length;
        } else {
            try (final InputStream is = new ByteArrayListInputStream(_bufs)) {
                is.skip(_pos);
                final int read_size = is.read(bytes);
                _pos += read_size;
                return read_size;
            }
        }
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
            log.warn("[{}]: pcm task ({}) paused already, ignore pause request", _sessionId, this);
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
            log.warn("[{}]: pcm task ({}) !NOT! paused, ignore resume request", _sessionId, this);
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
            // HubEventVO.sendEvent(_webSocket, "PlaybackStop", new PayloadPlaybackStop(0,"pcm", -1, _completed.get()));
        }
    }
}
