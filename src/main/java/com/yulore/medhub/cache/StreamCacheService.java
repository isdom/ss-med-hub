package com.yulore.medhub.cache;

import com.yulore.medhub.session.StreamSession;
import com.yulore.medhub.stream.BuildStreamTask;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

@Slf4j
@Component
public class StreamCacheService {

    /*
    oss_bucket: ylhz-aicall
    oss_path: aispeech/
    local_path: /var/znc/wav_cache/
    */

    private ExecutorService _scsExecutor;

    private final ConcurrentMap<String, LoadAndCahceTask> _key2task = new ConcurrentHashMap<>();

    @PostConstruct
    public void start() {
        _scsExecutor = Executors.newFixedThreadPool(NettyRuntime.availableProcessors() * 2,
                new DefaultThreadFactory("scsExecutor"));
    }

    @PreDestroy
    public void stop() {
        // 关闭客户端 - 注：关闭后不支持再次 start, 一般伴随JVM关闭而关闭
        _scsExecutor.shutdownNow();
    }

    public void asLocal(final BuildStreamTask streamTask, final Consumer<StreamSession> consumer) {
        final String key = streamTask.key();
        log.info("asLocal: try get Stream for {}", key);
        final LoadAndCahceTask task = _key2task.get(key);
        if (task != null) {
            task.onCached(consumer);
            log.info("asLocal: {} hit_cache_direct", key);
            return;
        }
        // not load before, try to load
        final LoadAndCahceTask myTask = new LoadAndCahceTask(streamTask);
        final LoadAndCahceTask prevTask = _key2task.putIfAbsent(key, myTask);
        if (prevTask != null) {
            // another has start task already
            prevTask.onCached(consumer);
            log.info("asLocal: {} follow_another_task", key);
            return;
        }
        log.info("asLocal: {} its_my_task", key);
        // it's me, first start task
        myTask.onCached(consumer);
        _scsExecutor.submit(myTask::start);
    }

    static class LoadAndCahceTask {
        private final Lock _lock = new ReentrantLock();
        private byte[] _bytes = null;
        private final List<Consumer<StreamSession>> _consumers = new ArrayList<>();
        private final BuildStreamTask _streamTask;

        public LoadAndCahceTask(final BuildStreamTask streamTask) {
            _streamTask = streamTask;
        }

        public void start() {
            final byte[] bytes = _streamTask.buildStream();

            List<Consumer<StreamSession>> todo;
            _lock.lock();
            _bytes = bytes;
            todo = new ArrayList<>(_consumers);
            _consumers.clear();
            _lock.unlock();
            for (Consumer<StreamSession> consumer : todo) {
                processConsumer(consumer);
            }
        }

        public void onCached(final Consumer<StreamSession> consumer) {
            _lock.lock();
            if (_bytes != null) {
                _lock.unlock();
                processConsumer(consumer);
            } else {
                _consumers.add(consumer);
                _lock.unlock();
            }
        }

        private void processConsumer(final Consumer<StreamSession> consumer) {
            try {
                consumer.accept(new StreamSession(new ByteArrayInputStream(_bytes), _bytes.length));
            } catch (Exception ex) {
                log.warn("exception when process consumer.accept: {}", ex.toString());
            }
        }

    }
}
