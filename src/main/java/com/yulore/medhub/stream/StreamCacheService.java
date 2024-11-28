package com.yulore.medhub.stream;

import com.google.common.primitives.Bytes;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
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

    public BuildStreamTask asCache(final BuildStreamTask sourceTask) {
        return new BuildStreamTask() {
            @Override
            public String key() {
                return null;
            }

            @Override
            public void buildStream(final Consumer<byte[]> onPart, final Consumer<Boolean> onCompleted) {
                final String key = sourceTask.key();
                log.info("asCache: try get Stream for {}", key);
                final LoadAndCahceTask task = _key2task.get(key);
                if (task != null) {
                    task.onCached(onPart, onCompleted);
                    log.info("asCache: {} hit_cache_direct", key);
                    return;
                }
                // not load before, try to load
                final LoadAndCahceTask myTask = new LoadAndCahceTask(sourceTask);
                final LoadAndCahceTask prevTask = _key2task.putIfAbsent(key, myTask);
                if (prevTask != null) {
                    // another has start task already
                    prevTask.onCached(onPart, onCompleted);
                    log.info("asCache: {} follow_another_task", key);
                    return;
                }
                log.info("asCache: {} its_my_task", key);
                // it's me, first start task
                myTask.onCached(onPart, onCompleted);
                _scsExecutor.submit(myTask::start);
            }
        };
    }

    static class LoadAndCahceTask {
        static final byte[] EMPTY_BYTES = new byte[0];
        private final Lock _lock = new ReentrantLock();
        private List<byte[]> _bytesList = null;
        private final List<Pair<Consumer<byte[]>, Consumer<Boolean>>> _consumers = new ArrayList<>();
        private final BuildStreamTask _souceTask;

        public LoadAndCahceTask(final BuildStreamTask sourceTask) {
            _souceTask = sourceTask;
        }

        public void start() {
            final AtomicReference<byte[]> bytesRef = new AtomicReference<>(EMPTY_BYTES);
            _souceTask.buildStream(
                    (bytes) -> bytesRef.set(Bytes.concat(bytesRef.get(), bytes)),
                    (isOK) -> {
                        List<Pair<Consumer<byte[]>, Consumer<Boolean>>> todo;
                        _lock.lock();
                        _bytes = bytesRef.get();
                        todo = new ArrayList<>(_consumers);
                        _consumers.clear();
                        _lock.unlock();
                        for (Pair<Consumer<byte[]>, Consumer<Boolean>> consumer : todo) {
                            processConsumer(consumer.getLeft(), consumer.getRight());
                        }
                    });
        }

        public void onCached(final Consumer<byte[]> onPart, final Consumer<Boolean> onCompleted) {
            _lock.lock();
            if (_bytesList != null) {
                _lock.unlock();
                processConsumer(onPart, onCompleted);
            } else {
                _consumers.add(Pair.of(onPart, onCompleted));
                _lock.unlock();
            }
        }

        private void processConsumer(final Consumer<byte[]> onPart, final Consumer<Boolean> onCompleted) {
            try {
                for (byte[] bytes : _bytesList) {
                    onPart.accept(_bytes);
                }
                // TODO: fix later for stream
                onCompleted.accept(true);
            } catch (Exception ex) {
                log.warn("exception when process consumer.accept: {}", ex.toString());
            }
        }
    }
}
