package com.yulore.medhub.task;

import com.yulore.medhub.vo.HubEventVO;
import com.yulore.medhub.vo.PayloadPlaybackStart;
import com.yulore.medhub.vo.PayloadPlaybackStop;
import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@AllArgsConstructor
@ToString
@Slf4j
public class PlayPCMTask {
    final ScheduledExecutorService _executor;
    final InputStream _is;
    final int _lenInBytes;
    final int _sampleRate;
    final int _interval;
    final int _channels;
    final WebSocket _webSocket;
    final Consumer<PlayPCMTask> _onEnd;
    final AtomicReference<ScheduledFuture<?>> _currentFuture = new AtomicReference<>(null);

    final AtomicBoolean _started = new AtomicBoolean(false);
    final AtomicBoolean _stopped = new AtomicBoolean(false);
    final AtomicBoolean _stopEventSended = new AtomicBoolean(false);

    public void start() {
        if (_stopped.get()) {
            log.warn("pcm task has stopped, can't start again");
        }
        if (_started.compareAndSet(false, true)) {
            HubEventVO.sendEvent(_webSocket, "PlaybackStart", new PayloadPlaybackStart("pcm", _sampleRate, _interval, _channels));
            schedule(1, System.currentTimeMillis());
        } else {
            log.warn("pcm task started, ignore multi-call start()");
        }
    }

    private void schedule(final int idx, final long startTimestamp) {
        if (_stopped.get()) {
            // ignore if stopped flag set
            return;
        }
        try {
            ScheduledFuture<?> current = null;
            final byte[] bytes = new byte[_lenInBytes];
            final int readSize = _is.read(bytes);
            log.info("PlayPCMTask {}: schedule read {} bytes", idx, readSize);
            final long delay = startTimestamp + (long) _interval * idx - System.currentTimeMillis();
            if (readSize == _lenInBytes) {
                current = _executor.schedule(() -> {
                    _webSocket.send(bytes);
                    schedule(idx+1, startTimestamp);
                },  delay, TimeUnit.MILLISECONDS);
            } else {
                _is.close();
                current = _executor.schedule(()->{
                            safeSendPlaybackStopEvent();
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

    public void stop() {
        if (_stopped.compareAndSet(false, true)) {
            if (_started.get()) {
                final ScheduledFuture<?> current = _currentFuture.getAndSet(null);
                if (null != current) {
                    current.cancel(false);
                }
                safeSendPlaybackStopEvent();
            }
        }
    }

    private void safeSendPlaybackStopEvent() {
        if (_stopEventSended.compareAndSet(false, true)) {
            HubEventVO.sendEvent(_webSocket, "PlaybackStop", new PayloadPlaybackStop("pcm"));
        }
    }
}
