package com.yulore.medhub.task;

import com.yulore.medhub.vo.WSEventVO;
import com.yulore.medhub.vo.PayloadPlaybackStart;
import com.yulore.medhub.vo.PayloadPlaybackStop;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@RequiredArgsConstructor
@ToString
@Slf4j
public class PlayPCMTask {
    final int _id;
    final int _initialSamples;
    final ScheduledExecutorService _executor;
    final InputStream _is;
    final SampleInfo _sampleInfo;
    final WebSocket _webSocket;
    final Consumer<PlayPCMTask> _onEnd;

    int _lenInBytes;
    final AtomicReference<ScheduledFuture<?>> _currentFuture = new AtomicReference<>(null);
    final AtomicInteger _currentIdx = new AtomicInteger(0);
    private long _startTimestamp;
    private long _pauseTimestamp;

    final AtomicBoolean _started = new AtomicBoolean(false);
    final AtomicBoolean _stopped = new AtomicBoolean(false);
    final AtomicBoolean _stopEventSended = new AtomicBoolean(false);
    final AtomicInteger _samples = new AtomicInteger(0);

    public void start() {
        _lenInBytes = _sampleInfo.bytesPerInterval();
        _samples.set(_initialSamples);
        if (_stopped.get()) {
            log.warn("pcm task has stopped, can't start again");
        }
        if (_started.compareAndSet(false, true)) {
            WSEventVO.sendEvent(_webSocket, "PlaybackStart", new PayloadPlaybackStart(_id,"pcm", _sampleInfo.sampleRate(), _sampleInfo.interval(), _sampleInfo.channels()));
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
        try {
            _currentIdx.set(idx);
            ScheduledFuture<?> current = null;
            final byte[] bytes = new byte[_lenInBytes];
            final int readSize = _is.read(bytes);
            // log.info("PlayPCMTask {}: schedule read {} bytes", idx, readSize);
            final long delay = _startTimestamp + (long) _sampleInfo.interval() * idx - System.currentTimeMillis();
            if (readSize == _lenInBytes) {
                current = _executor.schedule(() -> {
                    _webSocket.send(bytes);
                    _samples.addAndGet(_sampleInfo.sampleRate() / (1000 / _sampleInfo.interval()));
                    schedule(idx+1);
                },  delay, TimeUnit.MILLISECONDS);
            } else {
                // _is.close();
                current = _executor.schedule(()->{
                            safeSendPlaybackStopEvent(true);
                            _onEnd.accept(this);
                            log.info("schedule: schedule playback by {} send action", idx);
                        },
                        delay, TimeUnit.MILLISECONDS);
            }
            if (_stopped.get()) {
                // cancel if stopped flag set
                current.cancel(false);
            } else {
                _currentFuture.set(current);
            }
        } catch (IOException ex) {
            log.warn("schedule: {}", ex.toString());
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
                safeSendPlaybackStopEvent(false);
            }
        }
    }

    private void safeSendPlaybackStopEvent(final boolean completed) {
        if (_stopEventSended.compareAndSet(false, true)) {
            WSEventVO.sendEvent(_webSocket, "PlaybackStop", new PayloadPlaybackStop(_id,"pcm", _samples.get(), completed));
        }
    }
}
