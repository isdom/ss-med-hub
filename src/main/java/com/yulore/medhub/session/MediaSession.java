package com.yulore.medhub.session;

import com.alibaba.nls.client.protocol.asr.SpeechTranscriber;
import com.yulore.medhub.nls.ASRAgent;
import com.yulore.medhub.task.PlayPCMTask;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.Data;
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

@Data
@ToString
@Slf4j
public class MediaSession {
    public MediaSession(final boolean testEnableDelay, final long testDelayMs) {
        if (testEnableDelay) {
            _delayExecutor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("delayExecutor"));
            _testDelayMs = testDelayMs;
        }
    }

    public void lock() {
        _lock.lock();
        log.info("lock session: {}", _lock);
    }

    public void unlock() {
        log.info("unlock session: {}", _lock);
        _lock.unlock();
    }

    public void scheduleCheckIdle(final ScheduledExecutorService executor, final long delay, final Runnable sendCheckEvent) {
        _checkIdleFuture.set(executor.schedule(()->{
            try {
                sendCheckEvent.run();
                scheduleCheckIdle(executor, delay, sendCheckEvent);
            } catch (WebsocketNotConnectedException ex) {
                log.info("ws disconnected when sendCheckEvent: {}, stop checkIdle", ex.toString());
            } catch (Exception ex) {
                log.warn("exception when sendCheckEvent: {}, stop checkIdle", ex.toString());
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
        ASRAgent agent = null;
        try {
            lock();

            if (asrAgent == null) {
                log.info("stopAndCloseTranscriber: {} has already stopAndCloseTranscriber, ignore", webSocket.getRemoteSocketAddress());
                return;
            }
            agent = asrAgent;
            asrAgent = null;

            agent.decConnection();

            if (speechTranscriber != null) {
                try {
                    //通知服务端语音数据发送完毕，等待服务端处理完成。
                    long now = System.currentTimeMillis();
                    log.info("transcriber wait for complete");
                    speechTranscriber.stop();
                    log.info("transcriber stop() latency : {} ms", System.currentTimeMillis() - now);
                } catch (Exception ex) {
                    log.warn("handleStopAsrCommand error: {}", ex.toString());
                }

                speechTranscriber.close();
            }
            if (isTranscriptionStarted()) {
                // 对于已经标记了 TranscriptionStarted 的会话, 将其使用的 ASR Account 已连接通道减少一
                agent.decConnected();
            }
        } finally {
            unlock();
            if (agent != null) {
                log.info("release asr({}): {}/{}", agent.getName(), agent.get_connectingOrConnectedCount().get(), agent.getLimit());
            }
        }
    }

    public void transmit(final ByteBuffer bytes) {
        if (speechTranscriber != null) {
            if (_delayExecutor == null) {
                speechTranscriber.send(bytes.array());
            } else {
                _delayExecutor.schedule(()->speechTranscriber.send(bytes.array()), _testDelayMs, TimeUnit.MILLISECONDS);
            }
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

    public int addPlaybackStream(final byte[] bytes) {
        final int id = _playbackId.incrementAndGet();
        _id2stream.put(id, bytes);
        return id;
    }

    public byte[] getPlaybackStream(final int id) {
        return _id2stream.get(id);
    }

    private final Lock _lock = new ReentrantLock();

    SpeechTranscriber speechTranscriber;
    ASRAgent asrAgent;

    final AtomicBoolean _isStartTranscription = new AtomicBoolean(false);
    final AtomicBoolean _isTranscriptionStarted = new AtomicBoolean(false);
    ScheduledExecutorService _delayExecutor = null;
    long _testDelayMs = 0;

    final AtomicBoolean _isPlaying = new AtomicBoolean(false);
    final AtomicReference<PlayPCMTask> _playingTask = new AtomicReference<>(null);
    final AtomicInteger _playbackId = new AtomicInteger(0);
    final ConcurrentMap<Integer, byte[]> _id2stream = new ConcurrentHashMap<>();
    final AtomicReference<ScheduledFuture<?>>   _checkIdleFuture = new AtomicReference<>(null);
}
