package com.yulore.medhub.ws.actor;

import com.alibaba.nls.client.protocol.asr.SpeechTranscriber;
import com.yulore.medhub.vo.PayloadSentenceBegin;
import com.yulore.medhub.vo.PayloadSentenceEnd;
import com.yulore.medhub.vo.PayloadTranscriptionResultChanged;
import io.micrometer.core.instrument.Timer;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.exceptions.WebsocketNotConnectedException;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

@ToString
@Slf4j
public class ASRActor {
    @Builder
    @Data
    @ToString
    public static class PlaybackMemo {
        public final int playbackIdx;
        public final String contentId;
        public final boolean cancelOnSpeak;
        public final boolean hangup;
        public final Timer.Sample sampleWhenCreate;
        public long beginInMs;
    }

    public static int countChinesePunctuations(final String text) {
        int count = 0;
        for (char c : text.toCharArray()) {
            // 判断是否是中文标点
            if (isChinesePunctuation(c)) {
                count++;
            }
        }
        return count;
    }

    // 将需要计算的中文标点放入此数组中
    private static final Character[] punctuations = new Character[] {
            '，', '。', '！', '？', '；', '：', '“', '”'
    };

    private static boolean isChinesePunctuation(final char ch) {
        return Arrays.stream(punctuations).anyMatch(p -> p == ch);
    }

    public ASRActor() {
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

    public boolean transcriptionStarted() {
        return _isTranscriptionStarted.compareAndSet(false, true);
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

    public void notifySentenceBegin(final PayloadSentenceBegin payload) {
    }

    public void notifyTranscriptionResultChanged(final PayloadTranscriptionResultChanged payload) {
    }

    public void notifySentenceEnd(final PayloadSentenceEnd payload) {
    }

    public void close() {
        final ScheduledFuture<?> future = _checkIdleFuture.getAndSet(null);
        if (future != null) {
            future.cancel(false);
        }
        log.info("{} 's ASRSession close(), lasted: {} s, check idle {} times",
                _sessionId, (System.currentTimeMillis() - _sessionBeginInMs) / 1000.0f, _checkIdleCount.get());
    }

    public SpeechTranscriber onSpeechTranscriberCreated(final SpeechTranscriber speechTranscriber) {
        return speechTranscriber;
    }

    public void setASR(final Runnable stopASR, final Consumer<ByteBuffer> transmitData) {
        _stopASR.set(stopASR);
        _transmitData.set(transmitData);
    }

    public void createPlaybackMemo(final String playbackId,
                                    final Long contentId,
                                    final boolean cancelOnSpeak,
                                    final boolean hangup) {
        _id2memo.put(playbackId,
                PlaybackMemo.builder()
                        .playbackIdx(_playbackIdx.incrementAndGet())
                        .contentId(Long.toString(contentId))
                        .beginInMs(System.currentTimeMillis())
                        .cancelOnSpeak(cancelOnSpeak)
                        .hangup(hangup)
                        .sampleWhenCreate(Timer.start())
                        .build());
    }

    public PlaybackMemo memoFor(final String playbackId) {
        return _id2memo.get(playbackId);
    }

    public String _sessionId;
    public final Lock _lock = new ReentrantLock();

    final AtomicReference<Runnable> _stopASR = new AtomicReference<>(null);
    public final AtomicReference<Consumer<ByteBuffer>> _transmitData = new AtomicReference<>(null);

    final AtomicBoolean _isStartTranscription = new AtomicBoolean(false);
    public final AtomicBoolean _isTranscriptionStarted = new AtomicBoolean(false);
    public final AtomicBoolean _isTranscriptionFailed = new AtomicBoolean(false);
    public final AtomicInteger _transmitCount = new AtomicInteger(0);

    public final AtomicReference<ScheduledFuture<?>>   _checkIdleFuture = new AtomicReference<>(null);
    public final AtomicInteger _checkIdleCount = new AtomicInteger(0);
    public final long _sessionBeginInMs;

    private final AtomicInteger _playbackIdx = new AtomicInteger(0);
    private final ConcurrentMap<String, PlaybackMemo> _id2memo = new ConcurrentHashMap<>();
}
