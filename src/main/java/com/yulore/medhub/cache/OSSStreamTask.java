package com.yulore.medhub.cache;


import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.OSSObject;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Slf4j
public class OSSStreamTask implements BuildStreamTask {
    public OSSStreamTask(final String path, final OSS ossClient) {
        _ossClient = ossClient;
        // eg: {bucket=ylhz-aicall,url=ws://172.18.86.131:6789/playback,vars_playback_id=<uuid>,content_id=2088788,vars_start_timestamp=1732028219711854}
        //          aispeech/dd_app_sb_3_0/c264515130674055869c16fcc2458109.wav
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

        _bucketName = VarsUtil.extractValue(vars, "bucket");
        if (null == _bucketName) {
            log.warn("{} missing bucket field, ignore", path);
            return;
        }

        _objectName = path.substring(rightBracePos + 1);
        _key = _objectName.replace('/', '_');
    }

    @Override
    public String key() {
        return _key;
    }

    @Override
    public byte[] buildStream() {
        log.info("start load: {} from bucket: {}", _objectName, _bucketName);
        byte[] bytes;
        final long startInMs = System.currentTimeMillis();
        try (final OSSObject ossObject = _ossClient.getObject(_bucketName, _objectName);
             final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            ossObject.getObjectContent().transferTo(os);
            bytes = os.toByteArray();
            log.info("and save content size {}, total cost: {} ms", bytes.length, System.currentTimeMillis() - startInMs);
        } catch (IOException ex) {
            log.warn("start failed: {}", ex.toString());
            throw new RuntimeException(ex);
        }
        return bytes;
    }

    private final OSS _ossClient;
    private String _bucketName;
    private String _objectName;
    private String _key;
}
