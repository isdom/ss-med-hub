package com.yulore.medhub.task;

import com.yulore.medhub.vo.HubEventVO;
import com.yulore.medhub.vo.PayloadPlaybackStart;
import com.yulore.medhub.vo.PayloadPlaybackStop;
import com.yulore.util.ByteArrayListInputStream;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;

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
@ToString(of={"_taskId", "_path", "_completed"})
@Slf4j
public class PlayStreamPCMTask {
    private final String _taskId = UUID.randomUUID().toString();
    final String _path;
    final ScheduledExecutorService _executor;
    final SampleInfo _sampleInfo;
    // final WebSocket _webSocket;
    private final Consumer<Long> _onStartSend;
    private final Consumer<Long> _onStopSend;
    private final Consumer<byte[]> _doSendData;
    private final AtomicLong _startSendTimestamp = new AtomicLong(0);

    private final Consumer<PlayStreamPCMTask> _onEnd;
    private final AtomicBoolean _completed = new AtomicBoolean(false);

    int _lenInBytes;
    final AtomicReference<ScheduledFuture<?>> _currentFuture = new AtomicReference<>(null);
    final AtomicInteger _currentIdx = new AtomicInteger(0);
    private long _startTimestamp;
    private long _pauseTimestamp;

    final AtomicBoolean _started = new AtomicBoolean(false);
    final AtomicBoolean _stopped = new AtomicBoolean(false);
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
        _lenInBytes = _sampleInfo.lenInBytes();
        if (_stopped.get()) {
            log.warn("pcm task has stopped, can't start again");
        }
        if (_started.compareAndSet(false, true)) {
            log.info("pcm task for: {} start to playback", _path);
            // HubEventVO.sendEvent(_webSocket, "PlaybackStart", new PayloadPlaybackStart(0,"pcm", _sampleInfo.sampleRate, _sampleInfo.interval, _sampleInfo.channels));
            _startTimestamp = System.currentTimeMillis();
            schedule(1 );
        } else {
            log.warn("pcm task started, ignore multi-call start()");
        }
    }

    private void schedule(final int idx) {
        if (_stopped.get()) {
            // ignore if stopped flag set
            return;
        }
        _currentIdx.set(idx);
        ScheduledFuture<?> current = null;
        try {
            _lock.lock();
            if (_pos + _lenInBytes > _length && _streaming) {
                // need more data
                final long delay = _startTimestamp + (long) _sampleInfo.interval * idx - System.currentTimeMillis();
                current = _executor.schedule(() -> schedule(idx + 1), delay, TimeUnit.MILLISECONDS);
            } else {
                final byte[] bytes = new byte[_lenInBytes];
                final InputStream is = new ByteArrayListInputStream(_bufs);
                is.skip(_pos);
                final int readSize = is.read(bytes);
                _pos += readSize;
                final long delay = _startTimestamp + (long) _sampleInfo.interval * idx - System.currentTimeMillis();
                if (readSize == _lenInBytes) {
                    current = _executor.schedule(() -> {
                        if (_startSendTimestamp.compareAndSet(0, 1)) {
                            _startSendTimestamp.set(System.currentTimeMillis());
                            _onStartSend.accept(_startSendTimestamp.get());
                        }
                        _doSendData.accept(bytes);
                        schedule(idx + 1);
                    }, delay, TimeUnit.MILLISECONDS);
                } else {
                    // _is.close();
                    current = _executor.schedule(() -> {
                                _completed.compareAndSet(false, true);
                                safeSendPlaybackStopEvent();
                                log.info("schedule: schedule playback by {} send action", idx);
                            },
                            delay, TimeUnit.MILLISECONDS);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            _lock.unlock();
        }
        if (_stopped.get()) {
            // cancel if stopped flag set
            current.cancel(false);
        } else {
            _currentFuture.set(current);
        }
    }

    public void pause() {
        if (_stopped.get()) {
            // ignore if stopped flag set
            return;
        }
        final ScheduledFuture<?> current = _currentFuture.getAndSet(null);
        if (null != current) {
            current.cancel(true);
            _startSendTimestamp.set(0);
            _onStopSend.accept(System.currentTimeMillis());
        }
        _pauseTimestamp = System.currentTimeMillis();
    }

    public void resume() {
        if (_stopped.get()) {
            // ignore if stopped flag set
            return;
        }
        _startTimestamp += System.currentTimeMillis() - _pauseTimestamp;
        schedule(_currentIdx.get());
    }

    public void stop() {
        if (_stopped.compareAndSet(false, true)) {
            if (_started.get()) {
                final ScheduledFuture<?> current = _currentFuture.getAndSet(null);
                if (null != current) {
                    current.cancel(false);
                }
                safeSendPlaybackStopEvent();
            }
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
