package com.yulore.medhub.task;

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

@AllArgsConstructor
@ToString
@Slf4j
public class PlayPCMTask {
    final ScheduledExecutorService _executor;
    final InputStream _is;
    final int _lenInBytes;
    final int _interval;
    final WebSocket _webSocket;
    final Runnable _onEnd;

    public void start() {
        schedule(1, System.currentTimeMillis());
    }

    private void schedule(final int idx, final long startTimestamp) {
        try {
            final byte[] bytes = new byte[_lenInBytes];
            final int readSize = _is.read(bytes);
            log.info("PlayPCMTask {}: schedule read {} bytes", idx, readSize);
            final long delay = startTimestamp + (long) _interval * idx - System.currentTimeMillis();
            if (readSize == _lenInBytes) {
                final ScheduledFuture<?> future = _executor.schedule(() -> {
                    _webSocket.send(bytes);
                    schedule(idx+1, startTimestamp);
                },  delay, TimeUnit.MILLISECONDS);
                // futures.add(future);
            } else {
                _is.close();
                //futures.add(
                _executor.schedule(()->{
                            _onEnd.run();
                            log.info("schedule: schedule playback by {} send action", idx);
                        },
                        delay, TimeUnit.MILLISECONDS);
                //);
            }
        } catch (IOException ex) {
            log.warn("schedule: {}", ex.toString());
        }
    }

    public void stop() {

    }
}
