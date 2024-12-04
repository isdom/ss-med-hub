package com.yulore.medhub.session;

import com.yulore.medhub.task.PlayPCMTask;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;
import org.java_websocket.exceptions.WebsocketNotConnectedException;

import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

@ToString
@Slf4j
public class MediaSession {
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
                if (_testDisconnectTimeout > 0 && (System.currentTimeMillis() - _sessionBeginInMs > _testDisconnectTimeout)) {
                    _doDisconnect.run();
                    log.info("{}: do disconnect test feature", _sessionId);
                    return;
                }

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

    public void stopAndCloseTranscriber(final WebSocket webSocket) {
        // ASRAgent agent = null;
        final Runnable stopASR = _stopASR.getAndSet(null);
        if (stopASR != null) {
            try {
                lock();
                stopASR.run();

//                if (asrAgent == null) {
//                    log.info("stopAndCloseTranscriber: {} has already stopAndCloseTranscriber, ignore", webSocket.getRemoteSocketAddress());
//                    return;
//                }
//                agent = asrAgent;
//                asrAgent = null;
//
//                agent.decConnection();
//
//                if (speechTranscriber != null) {
//                    try {
//                        //通知服务端语音数据发送完毕，等待服务端处理完成。
//                        long now = System.currentTimeMillis();
//                        log.info("transcriber wait for complete");
//                        speechTranscriber.stop();
//                        log.info("transcriber stop() latency : {} ms", System.currentTimeMillis() - now);
//                    } catch (final Exception ex) {
//                        log.warn("handleStopAsrCommand error: {}", ex.toString());
//                    }
//
//                    speechTranscriber.close();
//                }
//                if (isTranscriptionStarted()) {
//                    // 对于已经标记了 TranscriptionStarted 的会话, 将其使用的 ASR Account 已连接通道减少一
//                    agent.decConnected();
//                }
            } finally {
                unlock();
//                if (agent != null) {
//                    log.info("release asr({}): {}/{}", agent.getName(), agent.get_connectingOrConnectedCount().get(), agent.getLimit());
//                }
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

    public int transmitCount() {
        return _transmitCount.get();
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

    public void setASR(final Runnable stopASR, final Consumer<ByteBuffer> transmitData) {
        _stopASR.set(stopASR);
        _transmitData.set(transmitData);
    }

    private final String _sessionId;
    private final Lock _lock = new ReentrantLock();

    // SpeechTranscriber speechTranscriber;
    // ASRAgent asrAgent;
    private AtomicReference<Runnable> _stopASR = new AtomicReference<>(null);
    private AtomicReference<Consumer<ByteBuffer>> _transmitData = new AtomicReference<>(null);

    final AtomicBoolean _isStartTranscription = new AtomicBoolean(false);
    final AtomicBoolean _isTranscriptionStarted = new AtomicBoolean(false);
    final AtomicBoolean _isTranscriptionFailed = new AtomicBoolean(false);
    final AtomicInteger _transmitCount = new AtomicInteger(0);
    ScheduledExecutorService _delayExecutor = null;
    long _testDelayMs = 0;
    long _testDisconnectTimeout = -1;
    final Runnable _doDisconnect;

    final AtomicBoolean _isPlaying = new AtomicBoolean(false);
    final AtomicReference<PlayPCMTask> _playingTask = new AtomicReference<>(null);
    final AtomicInteger _playbackId = new AtomicInteger(0);
    final ConcurrentMap<Integer, byte[]> _id2stream = new ConcurrentHashMap<>();
    final AtomicReference<ScheduledFuture<?>>   _checkIdleFuture = new AtomicReference<>(null);
    final AtomicInteger _checkIdleCount = new AtomicInteger(0);
    final long _sessionBeginInMs;
}
