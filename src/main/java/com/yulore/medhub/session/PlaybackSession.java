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
public class PlaybackSession {
    public PlaybackSession(final String sessionId) {
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

    private final String _sessionId;
    private final Lock _lock = new ReentrantLock();

    final AtomicBoolean _isPlaying = new AtomicBoolean(false);
    final AtomicReference<PlayPCMTask> _playingTask = new AtomicReference<>(null);
    final AtomicInteger _playbackId = new AtomicInteger(0);
    final ConcurrentMap<Integer, byte[]> _id2stream = new ConcurrentHashMap<>();
    final long _sessionBeginInMs;
}
