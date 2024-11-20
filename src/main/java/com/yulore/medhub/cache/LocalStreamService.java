package com.yulore.medhub.cache;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.OSSObject;
import com.yulore.medhub.session.StreamSession;
import com.yulore.util.ByteArrayListInputStream;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Slf4j
@Component
public class LocalStreamService {
    @Value("${oss.endpoint}")
    private String _oss_endpoint;

    @Value("${oss.access_key_id}")
    private String _oss_access_key_id;

    @Value("${oss.access_key_secret}")
    private String _oss_access_key_secret;

    private OSS _ossClient;

    /*
    oss_bucket: ylhz-aicall
    oss_path: aispeech/
    local_path: /var/znc/wav_cache/
    */

    private ExecutorService _lssExecutor;

    private final ConcurrentMap<String, LoadAndCahceTask> _key2task = new ConcurrentHashMap<>();

    @PostConstruct
    public void start() {
        _ossClient = new OSSClientBuilder().build(_oss_endpoint, _oss_access_key_id, _oss_access_key_secret);
        _lssExecutor = Executors.newFixedThreadPool(NettyRuntime.availableProcessors() * 2,
                new DefaultThreadFactory("lssExecutor"));
    }

    @PreDestroy
    public void stop() {
        // 关闭客户端 - 注：关闭后不支持再次 start, 一般伴随JVM关闭而关闭
        _lssExecutor.shutdownNow();
        _ossClient.shutdown();
    }

    public void asLocal(final String path, final Consumer<StreamSession> consumer) {
        // eg: {bucket=ylhz-aicall,url=ws://172.18.86.131:6789/playback,vars_playback_id=45af9503-a0ca-47d3-bb73-13a5afde65ea,content_id=2088788,vars_start_timestamp=1732028219711854}aispeech/dd_app_sb_3_0/c264515130674055869c16fcc2458109.wav
        final int leftBracePos = path.indexOf('{');
        if (leftBracePos == -1) {
            log.warn("{} missing vars, ignore", path);
            return;
        }
        final int rightBracePos = path.indexOf('}');
        if (rightBracePos == -1) {
            log.warn("{} missing vars, ignore", path);
            return;
        }
        final String vars = path.substring(leftBracePos + 1, rightBracePos);
        int bucketBeginIdx = vars.indexOf("bucket=");
        if (bucketBeginIdx == -1) {
            log.warn("{} missing bucket field, ignore", path);
            return;
        }

        int bucketEndIdx = vars.indexOf(',', bucketBeginIdx);

        final String bucketName = vars.substring(bucketBeginIdx + 7, bucketEndIdx == -1 ? vars.length() : bucketEndIdx);
        final String objectName = path.substring(rightBracePos + 1);
        final String key = objectName.replace('/', '_');

        log.info("asLocal: try get Stream for bucket:{}, objectName:{} as {}", bucketName, objectName, key);
        final LoadAndCahceTask task = _key2task.get(key);
        if (task != null) {
            task.onCached(consumer);
            log.info("asLocal: {} hit_cache_direct", key);
            return;
        }
        // not load before, try to load
        final LoadAndCahceTask myTask = new LoadAndCahceTask();
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
        _lssExecutor.submit(()->myTask.start(bucketName, objectName));
    }

    class LoadAndCahceTask {
        private final Lock _lock = new ReentrantLock();
        private byte[] _bytes = null;
        private final List<Consumer<StreamSession>> _consumers = new ArrayList<>();

        public void start(final String bucketName, final String objectName) {
            log.info("start load: {} from bucket: {}", objectName, bucketName);
            byte[] bytes;
            final long startInMs = System.currentTimeMillis();
            try (final OSSObject ossObject = _ossClient.getObject(bucketName, objectName);
                 final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                ossObject.getObjectContent().transferTo(os);
                bytes = os.toByteArray();
                log.info("and save content size {}, total cost: {} ms", bytes.length, System.currentTimeMillis() - startInMs);
            } catch (IOException ex) {
                log.warn("start failed: {}", ex.toString());
                throw new RuntimeException(ex);
            }

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
