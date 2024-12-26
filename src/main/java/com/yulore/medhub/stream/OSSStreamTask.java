package com.yulore.medhub.stream;


import com.aliyun.oss.OSS;
import com.aliyun.oss.model.OSSObject;
import lombok.extern.slf4j.Slf4j;

import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.UnsupportedAudioFileException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.function.Consumer;

@Slf4j
public class OSSStreamTask implements BuildStreamTask {
    public OSSStreamTask(final String path, final OSS ossClient, final boolean removeWavHdr) {
        _ossClient = ossClient;
        _removeWavHdr = removeWavHdr;
        // eg: {bucket=ylhz-aicall,url=ws://172.18.86.131:6789/playback,vars_playback_id=<uuid>,content_id=2088788,vars_start_timestamp=1732028219711854}
        //          aispeech/dd_app_sb_3_0/c264515130674055869c16fcc2458109.wav
        final int leftBracePos = path.indexOf('{');
        if (leftBracePos == -1) {
            log.warn("{} missing vars, ignore", path);
            throw new RuntimeException(path + " missing vars, ignore");
        }
        final int rightBracePos = path.indexOf('}');
        if (rightBracePos == -1) {
            log.warn("{} missing vars, ignore", path);
            throw new RuntimeException(path + " missing vars, ignore");
        }
        final String vars = path.substring(leftBracePos + 1, rightBracePos);

        _bucketName = VarsUtil.extractValue(vars, "bucket");
        if (null == _bucketName) {
            log.warn("{} missing bucket field, ignore", path);
            throw new RuntimeException(path + " missing bucket field, ignore");
        }

        _objectName = path.substring(rightBracePos + 1);
        _key = buildKey();
    }

    private String buildKey() {
        final StringBuilder sb = new StringBuilder();
        sb.append("oss-hdr:");
        sb.append(!_removeWavHdr);
        sb.append(":");
        sb.append(_bucketName);
        sb.append(":");
        sb.append(_objectName);
        return sb.toString();
    }

    @Override
    public String key() {
        return _key;
    }

    @Override
    public void buildStream(final Consumer<byte[]> onPart, final Consumer<Boolean> onCompleted) {
        log.info("start load: {} from bucket: {}", _objectName, _bucketName);
        byte[] bytes;
        final long startInMs = System.currentTimeMillis();
        try (final OSSObject ossObject = _ossClient.getObject(_bucketName, _objectName);
             final ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            ossObject.getObjectContent().transferTo(bos);
            if (_removeWavHdr) {
                try {
                    final ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
                    bos.reset();
                    AudioSystem.getAudioInputStream(bis).transferTo(bos);
                } catch (UnsupportedAudioFileException ex) {
                    log.warn("failed to extract pcm from wav: {}", ex.toString());
                }
            }
            bytes = bos.toByteArray();
            log.info("and save content size {}, total cost: {} ms", bytes.length, System.currentTimeMillis() - startInMs);
            onPart.accept(bytes);
            onCompleted.accept(true);
        } catch (IOException ex) {
            log.warn("start failed: {}", ex.toString());
            // throw new RuntimeException(ex);
            onCompleted.accept(false);
        }
    }

    private final OSS _ossClient;
    private final String _bucketName;
    private final String _objectName;
    private final String _key;
    private final boolean _removeWavHdr;
}
