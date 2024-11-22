package com.yulore.medhub.cache;


import com.aliyun.oss.OSS;
import com.aliyun.oss.model.OSSObject;
import com.mgnt.utils.StringUnicodeEncoderDecoder;
import com.yulore.medhub.nls.TTSAgent;
import com.yulore.medhub.nls.TTSTask;
import com.yulore.medhub.task.PlayPCMTask;
import com.yulore.medhub.task.SampleInfo;
import com.yulore.util.ByteArrayListInputStream;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class TTSStreamTask implements BuildStreamTask {
    public TTSStreamTask(final String path, final TTSAgent agent) {
        _agent = agent;
        // eg: {type=tts,url=ws://172.18.86.131:6789/playback,vars_playback_id=<uuid>,content_id=2088788,vars_start_timestamp=1732028219711854}
        //          'StringUnicodeEncoderDecoder.encodeStringToUnicodeSequence(content)'.wav
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

        _key = path.substring(rightBracePos + 1, path.lastIndexOf(".wav"));
        _text = StringUnicodeEncoderDecoder.decodeUnicodeSequenceToString(_key);
    }

    @Override
    public String key() {
        return _key;
    }

    @Override
    public byte[] buildStream() {
        log.info("start gen tts: {}", _text);
        final List<byte[]> bufs = new ArrayList<>();
        final AtomicInteger idx = new AtomicInteger(0);
        final long startInMs = System.currentTimeMillis();

        final TTSTask task = new TTSTask(_agent, _text,
                (bytes) -> {
                    final byte[] bytesArray = new byte[bytes.remaining()];
                    bytes.get(bytesArray, 0, bytesArray.length);
                    bufs.add(bytesArray);
                    log.info("TTSStreamTask: {}: onData {} bytes", idx.incrementAndGet(), bytesArray.length);
                },
                (response)->{
                    log.info("TTSStreamTask: gen pcm stream cost={} ms", System.currentTimeMillis() - startInMs);
                },
                (response)-> log.warn("tts failed: {}", response));
        task.start();
        try {
            task.waitForComplete();
        } catch (Exception ignored) {
        }

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        try (InputStream is = new ByteArrayListInputStream(bufs)) {
            is.transferTo(os);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return os.toByteArray();
    }

    private final TTSAgent _agent;
    private String _key;
    private String _text;
}
