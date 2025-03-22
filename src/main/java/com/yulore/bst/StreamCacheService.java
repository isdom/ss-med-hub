package com.yulore.bst;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor
@Component
public class StreamCacheService {

    private final Function<String, Executor> executorProvider;
    private Executor executor;
    private final ConcurrentMap<String, LoadAndCahceTask> _key2task = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        executor = executorProvider.apply("ltx");
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
                executor.execute(myTask::start);
            }
        };
    }

    static class LoadAndCahceTask {
        private final Lock _lock = new ReentrantLock();
        private final List<byte[]> _bytesList = new ArrayList<>();
        final private AtomicBoolean _completed = new AtomicBoolean(false);
        private final List<Pair<Consumer<byte[]>, Consumer<Boolean>>> _consumers = new ArrayList<>();
        private final BuildStreamTask _sourceTask;

        public LoadAndCahceTask(final BuildStreamTask sourceTask) {
            _sourceTask = sourceTask;
        }

        public void start() {
            _sourceTask.buildStream(
                    (bytes) -> {
                        try {
                            _lock.lock();
                            _bytesList.add(bytes);
                            for (Pair<Consumer<byte[]>, Consumer<Boolean>> consumer : _consumers) {
                                // feed new received bytes to all consumers
                                try {
                                    consumer.getLeft().accept(bytes);
                                } catch (Exception ignored) {
                                }
                            }
                        } finally {
                            _lock.unlock();
                        }
                    },
                    (isOK) -> {
                        if (_completed.compareAndSet(false, true)) {
                            List<Pair<Consumer<byte[]>, Consumer<Boolean>>> todo;
                            _lock.lock();
                            todo = new ArrayList<>(_consumers);
                            _consumers.clear();
                            _lock.unlock();
                            for (Pair<Consumer<byte[]>, Consumer<Boolean>> consumer : todo) {
                                try {
                                    consumer.getRight().accept(true);
                                } catch (Exception ignored) {
                                }
                            }
                        }
                    });
        }

        public void onCached(final Consumer<byte[]> onPart, final Consumer<Boolean> onCompleted) {
            if (_completed.get()) {
                // has completed, lock free and feed all bytes list via onPart and call onCompleted
                processConsumer(onPart, onCompleted);
                return;
            }

            // not completed
            try {
                _lock.lock();
                // inside lock
                processConsumer(onPart, onCompleted);
                if (!_completed.get()) {
                    // check again isComplete, if not, register consumers for later call
                    _consumers.add(Pair.of(onPart, onCompleted));
                }
            } finally {
                _lock.unlock();
            }
        }

        private void processConsumer(final Consumer<byte[]> onPart, final Consumer<Boolean> onCompleted) {
            try {
                for (byte[] bytes : _bytesList) {
                    onPart.accept(bytes);
                }
                if (_completed.get()) {
                    onCompleted.accept(true);
                }
            } catch (Exception ex) {
                log.warn("exception when process consumer.accept: {}", ex.toString());
            }
        }
    }
}
