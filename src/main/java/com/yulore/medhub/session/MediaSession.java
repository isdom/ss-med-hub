package com.yulore.medhub.session;

import com.alibaba.nls.client.protocol.asr.SpeechTranscriber;
import com.yulore.medhub.nls.ASRAgent;
import com.yulore.medhub.task.PlayPCMTask;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;

import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Data
@ToString
@Slf4j
public class MediaSession {
    public MediaSession(final boolean testEnableDelay, final long testDelayMs) {
        if (testEnableDelay) {
            _delayExecutor = Executors.newSingleThreadScheduledExecutor();
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
        try {
            lock();

            if (asrAgent == null) {
                log.info("stopAndCloseTranscriber: {} has already stopAndCloseTranscriber, ignore", webSocket.getRemoteSocketAddress());
                return;
            }
            final ASRAgent agent = asrAgent;
            asrAgent = null;

            agent.decConnection();

            if (speechTranscriber != null) {
                try {
                    //通知服务端语音数据发送完毕，等待服务端处理完成。
                    long now = System.currentTimeMillis();
                    log.info("transcriber wait for complete");
                    speechTranscriber.stop();
                    log.info("transcriber stop() latency : {} ms", System.currentTimeMillis() - now);
                } catch (Exception e) {
                    log.warn("handleStopAsrCommand error: {}", e.toString());
                }

                speechTranscriber.close();
            }
            if (isTranscriptionStarted()) {
                // 对于已经标记了 TranscriptionStarted 的会话, 将其使用的 ASR Account 已连接通道减少一
                agent.decConnected();
            }
        } finally {
            unlock();
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
    }

    public void stopCurrentAndStartPlay(final PlayPCMTask task) {
        final PlayPCMTask current = _playingTask.getAndSet(task);
        if (current != null) {
            current.stop();
        }
    }

    public void stopCurrent(final PlayPCMTask task) {
        if (_playingTask.compareAndSet(task, null)) {
            task.stop();
        }
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
}
