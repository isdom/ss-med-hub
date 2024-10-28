package com.yulore.asrhub.session;

import com.alibaba.nls.client.protocol.asr.SpeechTranscriber;
import com.yulore.asrhub.nls.ASRAccount;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Data
@ToString
@Slf4j
public class Session {
    private final Lock _lock = new ReentrantLock();

    SpeechTranscriber speechTranscriber;
    ASRAccount asrAccount;

    final AtomicBoolean _isStartTranscription = new AtomicBoolean(false);
    final AtomicBoolean _isTranscriptionStarted = new AtomicBoolean(false);

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

            if (asrAccount == null) {
                log.info("stopAndCloseTranscriber: {} has already stopAndCloseTranscriber, ignore", webSocket.getRemoteSocketAddress());
                return;
            }
            final ASRAccount asr = asrAccount;
            asrAccount = null;

            asr.decConnection();

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
                asr.decConnected();
            }
        } finally {
            unlock();
        }
    }
}
