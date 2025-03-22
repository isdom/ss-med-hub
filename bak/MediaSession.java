package com.yulore.medhub.session;

import com.yulore.medhub.task.PlayPCMTask;
import com.yulore.medhub.ws.actor.ASRActor;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@ToString
@Slf4j
public class MediaSession extends ASRActor {
    public MediaSession(final String sessionId, final boolean testEnableDelay, final long testDelayMs, final boolean testEnableDisconnect, final float testDisconnectProbability, final Runnable doDisconnect) {
        if (testEnableDelay) {
            _delayExecutor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("delayExecutor"));
            _testDelayMs = testDelayMs;
        }
        if (testEnableDisconnect && Math.random() < testDisconnectProbability) {
            _testDisconnectTimeout = (long)(Math.random() * 5000) + 5000L; // 5s ~ 10s
            log.info("{}: enable disconnect test feature, timeout: {} ms", sessionId, _testDisconnectTimeout);
        }
        _doDisconnect = doDisconnect;
        _sessionId = sessionId;
    }

    public boolean transmit(final ByteBuffer bytes) {
        if ( !_isTranscriptionStarted.get() || _isTranscriptionFailed.get()) {
            return false;
        }

        final Consumer<ByteBuffer> transmitter = _transmitData.get();

        if (transmitter != null) {
            if (_delayExecutor == null) {
                transmitter.accept(bytes);
            } else {
                _delayExecutor.schedule(()->transmitter.accept(bytes), _testDelayMs, TimeUnit.MILLISECONDS);
            }
            _transmitCount.incrementAndGet();
            return true;
        } else {
            return false;
        }
    }

    public void close() {
        if (_delayExecutor != null) {
            _delayExecutor.shutdownNow();
        }
        _id2stream.clear();
        final ScheduledFuture<?> future = _checkIdleFuture.getAndSet(null);
        if (future != null) {
            future.cancel(false);
        }
        log.info("{} 's MediaSession close(), lasted: {} s, check idle {} times",
                _sessionId, (System.currentTimeMillis() - _sessionBeginInMs) / 1000.0f, _checkIdleCount.get());
    }

    public void stopCurrentAndStartPlay(final PlayPCMTask current) {
        final PlayPCMTask previous = _playingTask.getAndSet(current);
        if (previous != null) {
            previous.stop();
        }
        if (current != null) {
            current.start();
        }
    }

    public void stopCurrentIfMatch(final PlayPCMTask current) {
        if (_playingTask.compareAndSet(current, null)) {
            if (current != null) {
                current.stop();
            }
        }
    }

    public void stopCurrentAnyway() {
        final PlayPCMTask current = _playingTask.getAndSet(null);
        if (current != null) {
            current.stop();
        }
    }

    public void pauseCurrentAnyway() {
        final PlayPCMTask current = _playingTask.get();
        if (current != null) {
            current.pause();
        }
    }

    public void resumeCurrentAnyway() {
        final PlayPCMTask current = _playingTask.get();
        if (current != null) {
            current.resume();
        }
    }

    public int addPlaybackStream(final byte[] bytes) {
        final int id = _playbackId.incrementAndGet();
        _id2stream.put(id, bytes);
        return id;
    }

    public byte[] getPlaybackStream(final int id) {
        return _id2stream.get(id);
    }

    ScheduledExecutorService _delayExecutor = null;
    long _testDelayMs = 0;
    long _testDisconnectTimeout = -1;
    final Runnable _doDisconnect;

    final AtomicBoolean _isPlaying = new AtomicBoolean(false);
    final AtomicReference<PlayPCMTask> _playingTask = new AtomicReference<>(null);
    final AtomicInteger _playbackId = new AtomicInteger(0);
    final ConcurrentMap<Integer, byte[]> _id2stream = new ConcurrentHashMap<>();
}
