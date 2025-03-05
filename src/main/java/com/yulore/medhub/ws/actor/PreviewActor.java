package com.yulore.medhub.ws.actor;

import com.yulore.medhub.task.PlayStreamPCMTask;
import com.yulore.medhub.ws.WsHandler;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@ToString
@Slf4j
public abstract class PreviewActor implements WsHandler  {
    @Override
    public void onAttached(WebSocket webSocket) {

    }

    @Override
    public void onClose(WebSocket webSocket) {

    }

    public PreviewActor() {
        _sessionBeginInMs = System.currentTimeMillis();
    }

    public void notifyPlaybackStart(final PlayStreamPCMTask task) {
        if (_playingTask.get() == task) {
            _isPlaying.set(true);
        } else {
            log.warn("PreviewSession.notifyPlaybackStart: playingTask: {} !NOT! match notify sourceTask: {}, ignore",
                    _playingTask.get(), task);
        }
    }

    public void notifyPlaybackStop(final PlayStreamPCMTask task) {
        if (_playingTask.compareAndSet(task, null)) {
            _isPlaying.set(false);
            _idleStartInMs.set(System.currentTimeMillis());
        } else if (_playingTask.get() != null) {
            log.warn("PreviewSession.notifyPlaybackStop: playingTask: {} !NOT! match notify sourceTask: {}, ignore",
                    _playingTask.get(), task);
        }
    }

    public boolean isPlaying() {
        return _isPlaying.get();
    }

    public long idleStartInMs() {
        return _idleStartInMs.get();
    }

    public void attach(final PlayStreamPCMTask current) {
        final PlayStreamPCMTask previous = _playingTask.getAndSet(null);
        if (previous != null) {
            previous.stop();
        }
        if (!_playingTask.compareAndSet(null, current)) {
            log.warn("attach {} failed, another task has attached", current);
        }
    }

    public void stopCurrent() {
        final PlayStreamPCMTask current = _playingTask.getAndSet(null);
        if (current != null) {
            current.stop();
        }
    }

    public void pauseCurrent() {
        final PlayStreamPCMTask current = _playingTask.get();
        if (current != null) {
            if (current.pause()) {
                log.info("task {} paused success", current);
            } else {
                log.warn("task {} paused failed", current);
            }
        }
    }

    public void resumeCurrent() {
        final PlayStreamPCMTask current = _playingTask.get();
        if (current != null && current.isPaused()) {
            current.resume();
            log.info("task {} resumed", current);
        }
    }

    final AtomicBoolean _isPlaying = new AtomicBoolean(false);
    final AtomicLong _idleStartInMs = new AtomicLong(System.currentTimeMillis());
    final AtomicReference<PlayStreamPCMTask> _playingTask = new AtomicReference<>(null);
    final long _sessionBeginInMs;
}
