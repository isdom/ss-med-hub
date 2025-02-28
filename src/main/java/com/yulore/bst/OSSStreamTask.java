package com.yulore.bst;


import com.aliyun.oss.OSS;
import com.aliyun.oss.model.OSSObject;
import com.google.common.base.Strings;
import com.yulore.util.VarsUtil;
import com.yulore.util.WaveUtil;
import lombok.extern.slf4j.Slf4j;

import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;
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

        final String start = VarsUtil.extractValue(vars, "start");
        _start = Strings.isNullOrEmpty(start) ? 0 : Integer.parseInt(start);

        final String end = VarsUtil.extractValue(vars, "end");
        _end = Strings.isNullOrEmpty(end) ? -1 : Integer.parseInt(end);

        _objectName = path.substring(rightBracePos + 1);

        final String cacheStr = VarsUtil.extractValue(vars, "cache");
        final boolean cache = !Strings.isNullOrEmpty(cacheStr) && Boolean.parseBoolean(cacheStr);
        _key = cache ? buildKey() : null;
    }

    private String buildKey() {
        final StringBuilder sb = new StringBuilder();
        sb.append("oss-hdr:");
        sb.append(!_removeWavHdr);
        sb.append(":");
        sb.append(_bucketName);
        sb.append(":");
        sb.append(_objectName);
        sb.append(":");
        sb.append(_start);
        sb.append(":");
        sb.append(_end);
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
            if (_start > 0 || _end != -1 || _removeWavHdr) {
                try {
                    final ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
                    bos.reset();
                    final AudioInputStream ais = AudioSystem.getAudioInputStream(bis);
                    final AudioFormat af = ais.getFormat();
                    final int samples = (int)ais.getFrameLength();
                    if (_start > 0) {
                        log.info("AudioFormat: frameSize:{}/sampleSizeInBits:{}", af.getFrameSize(), af.getSampleSizeInBits());
                        ais.skip((long) _start * af.getFrameSize());
                    }

                    if (!_removeWavHdr) {
                        // add wav header
                        bos.write(WaveUtil.genWaveHeader((int)af.getSampleRate(), af.getChannels()));
                    }
                    if (_end == -1) {
                        ais.transferTo(bos);
                    } else if (_end > samples) {
                        log.warn("{}'s total samples:{}, BUT end:{}, ignore end property", _objectName, samples, _end);
                        ais.transferTo(bos);
                    } else {
                        log.info("AudioFormat: frameSize:{}/sampleSizeInBits:{}", af.getFrameSize(), af.getSampleSizeInBits());
                        int to_transfer = (_end - _start) * af.getFrameSize();
                        byte[] buffer = new byte[512];
                        int read;
                        while (to_transfer > 0 && (read = ais.read(buffer, 0, Math.min(512, to_transfer))) >= 0) {
                            bos.write(buffer, 0, read);
                            to_transfer -= read;
                        }
                    }
                } catch (UnsupportedAudioFileException ex) {
                    log.warn("failed to extract pcm from wav: {}", ex.toString());
                }
            }
            bytes = bos.toByteArray();
            log.info("and save content (has wav hdr:{}/start:{}/end:{}) size {}, total cost: {} ms",
                    !_removeWavHdr, _start, _end,
                    bytes.length, System.currentTimeMillis() - startInMs);
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
    private final int _start;
    private final int _end;
}
