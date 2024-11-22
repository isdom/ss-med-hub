package com.yulore.medhub.stream;


import com.mgnt.utils.StringUnicodeEncoderDecoder;
import com.yulore.medhub.nls.TTSAgent;
import com.yulore.medhub.nls.TTSTask;
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
        // eg: {type=tts,voice=xxx,url=ws://172.18.86.131:6789/playback,vars_playback_id=<uuid>,content_id=2088788,vars_start_timestamp=1732028219711854}
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

        _voice = VarsUtil.extractValue(vars, "voice");
        _pitchRate = VarsUtil.extractValue(vars, "pitch_rate");
        _speechRate = VarsUtil.extractValue(vars, "speech_rate");
        final String key = path.substring(rightBracePos + 1, path.lastIndexOf(".wav"));
        _text = StringUnicodeEncoderDecoder.decodeUnicodeSequenceToString(key);
    }

    @Override
    public String key() {
        return null;
    }

    @Override
    public byte[] buildStream() {
        log.info("start gen tts: {}", _text);
        final List<byte[]> bufs = new ArrayList<>();
        final AtomicInteger idx = new AtomicInteger(0);
        final long startInMs = System.currentTimeMillis();

        final TTSTask task = new TTSTask(_agent,
                (synthesizer)->{
                    synthesizer.setText(_text);
                    if (null != _voice) {
                        synthesizer.setVoice(_voice);
                    }
                    if (null != _pitchRate) {
                        synthesizer.setPitchRate(Integer.parseInt(_pitchRate));
                    }
                    if (null != _speechRate) {
                        synthesizer.setSpeechRate(Integer.parseInt(_speechRate));
                    }
                },
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
    // private String _key;
    private String _text;
    private String _voice;
    private String _pitchRate;
    private String _speechRate;
}
