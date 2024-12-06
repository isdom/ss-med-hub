package com.yulore.medhub.session;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.exceptions.WebsocketNotConnectedException;

import java.nio.ByteBuffer;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

@ToString
@Slf4j
public class ASRSession {
    public ASRSession() {
        _sessionBeginInMs = System.currentTimeMillis();
    }

    public String sessionId() {
        return _sessionId;
    }

    public void lock() {
        _lock.lock();
    }

    public void unlock() {
        _lock.unlock();
    }

    public void scheduleCheckIdle(final ScheduledExecutorService executor, final long delay, final Runnable sendCheckEvent) {
        _checkIdleFuture.set(executor.schedule(()->{
            try {
                sendCheckEvent.run();
                _checkIdleCount.incrementAndGet();
                scheduleCheckIdle(executor, delay, sendCheckEvent);
            } catch (final WebsocketNotConnectedException ex) {
                log.info("{} 's ws disconnected when sendCheckEvent: {}, stop checkIdle", _sessionId, ex.toString());
            } catch (final Exception ex) {
                log.warn("{} exception when sendCheckEvent: {}, stop checkIdle", _sessionId, ex.toString());
            }
        }, delay, TimeUnit.MILLISECONDS));
    }

    public boolean startTranscription() {
        return _isStartTranscription.compareAndSet(false, true);
    }

    public void transcriptionStarted() {
        _isTranscriptionStarted.compareAndSet(false, true);
    }

    public boolean isTranscriptionStarted() {
        return _isTranscriptionStarted.get();
    }

    public void stopAndCloseTranscriber() {
        final Runnable stopASR = _stopASR.getAndSet(null);
        if (stopASR != null) {
            try {
                lock();
                stopASR.run();

            } finally {
                unlock();
            }
        }
    }

    public void notifySpeechTranscriberFail() {
        _isTranscriptionFailed.compareAndSet(false, true);
        _transmitData.set(null);
    }

    public boolean transmit(final ByteBuffer bytes) {
        if ( !_isTranscriptionStarted.get() || _isTranscriptionFailed.get()) {
            return false;
        }

        final Consumer<ByteBuffer> transmitter = _transmitData.get();

        if (transmitter != null) {
            transmitter.accept(bytes);
            _transmitCount.incrementAndGet();
            return true;
        } else {
            return false;
        }
    }

    public int transmitCount() {
        return _transmitCount.get();
    }

    public void close() {
        final ScheduledFuture<?> future = _checkIdleFuture.getAndSet(null);
        if (future != null) {
            future.cancel(false);
        }
        log.info("{} 's MediaSession close(), lasted: {} s, check idle {} times",
                _sessionId, (System.currentTimeMillis() - _sessionBeginInMs) / 1000.0f, _checkIdleCount.get());
    }

    public void setASR(final Runnable stopASR, final Consumer<ByteBuffer> transmitData) {
        _stopASR.set(stopASR);
        _transmitData.set(transmitData);
    }

    String _sessionId;
    final Lock _lock = new ReentrantLock();

    AtomicReference<Runnable> _stopASR = new AtomicReference<>(null);
    AtomicReference<Consumer<ByteBuffer>> _transmitData = new AtomicReference<>(null);

    final AtomicBoolean _isStartTranscription = new AtomicBoolean(false);
    final AtomicBoolean _isTranscriptionStarted = new AtomicBoolean(false);
    final AtomicBoolean _isTranscriptionFailed = new AtomicBoolean(false);
    final AtomicInteger _transmitCount = new AtomicInteger(0);

    final AtomicReference<ScheduledFuture<?>>   _checkIdleFuture = new AtomicReference<>(null);
    final AtomicInteger _checkIdleCount = new AtomicInteger(0);
    final long _sessionBeginInMs;
}
