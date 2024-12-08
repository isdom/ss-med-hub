package com.yulore.medhub.stream;


import com.alibaba.nacos.common.utils.MD5Utils;
import com.alibaba.nls.client.protocol.OutputFormatEnum;
import com.alibaba.nls.client.protocol.SampleRateEnum;
import com.alibaba.nls.client.protocol.tts.SpeechSynthesizer;
import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.mgnt.utils.StringUnicodeEncoderDecoder;
import com.yulore.medhub.nls.TTSAgent;
import com.yulore.medhub.nls.TTSTask;
import com.yulore.util.ByteArrayListInputStream;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

@Slf4j
public class TTSStreamTask implements BuildStreamTask {
    final HashFunction _MD5 = Hashing.md5();

    public TTSStreamTask(final String path, final Supplier<TTSAgent> getTTSAgent, final Consumer<SpeechSynthesizer> onSynthesizer) {
        _getTTSAgent = getTTSAgent;
        _onSynthesizer = onSynthesizer;

        // eg: {type=tts,voice=xxx,url=ws://172.18.86.131:6789/playback,vars_playback_id=<uuid>,
        //      content_id=2088788,vars_start_timestamp=1732028219711854,text='StringUnicodeEncoderDecoder.encodeStringToUnicodeSequence(content)'}
        //          playback_id.wav
        final int leftBracePos = path.indexOf('{');
        if (leftBracePos == -1) {
            log.warn("{} missing vars, ignore", path);
            throw new RuntimeException(path + " missing vars.");
        }
        final int rightBracePos = path.indexOf('}');
        if (rightBracePos == -1) {
            log.warn("{} missing vars, ignore", path);
            throw new RuntimeException(path + " missing vars.");
        }
        final String vars = path.substring(leftBracePos + 1, rightBracePos);

        _text = VarsUtil.extractValue(vars, "text");
        if (_text == null) {
            log.warn("{} missing text, ignore", path);
            throw new RuntimeException(path + " missing text.");
        }
        _text = StringUnicodeEncoderDecoder.decodeUnicodeSequenceToString(_text);

        _voice = VarsUtil.extractValue(vars, "voice");
        _pitchRate = VarsUtil.extractValue(vars, "pitch_rate");
        _speechRate = VarsUtil.extractValue(vars, "speech_rate");
        _cache = VarsUtil.extractValueAsBoolean(vars, "cache", false);
        _key = _cache ? buildKey() : null;
    }

    private String buildKey() {
        final StringBuilder sb = new StringBuilder();
        sb.append(_voice);
        sb.append(":");
        sb.append(_pitchRate);
        sb.append(":");
        sb.append(_speechRate);
        sb.append(":");
        sb.append(_text);

        return "tts-" + _MD5.hashString(sb.toString(), Charsets.UTF_8);
    }

    @Override
    public String key() {
        return _key;
    }

    @Override
    public void buildStream(final Consumer<byte[]> onPart, final Consumer<Boolean> onCompleted) {
        log.info("start gen tts: {}", _text);
        final AtomicInteger idx = new AtomicInteger(0);
        final long startInMs = System.currentTimeMillis();

        final TTSTask task = new TTSTask(_getTTSAgent.get(),
                (synthesizer)->{
                    synthesizer.setText(_text);
                    if (null != _voice && !_voice.isEmpty()) {
                        synthesizer.setVoice(_voice);
                    }
                    if (null != _pitchRate && !_pitchRate.isEmpty()) {
                        synthesizer.setPitchRate(Integer.parseInt(_pitchRate));
                    }
                    if (null != _speechRate && !_speechRate.isEmpty()) {
                        synthesizer.setSpeechRate(Integer.parseInt(_speechRate));
                    }

                    // 设置返回音频的编码格式
                    // synthesizer.setFormat(OutputFormatEnum.WAV);
                    // 设置返回音频的采样率
                    // synthesizer.setSampleRate(SampleRateEnum.SAMPLE_RATE_8K);

                    if (_onSynthesizer != null) {
                        _onSynthesizer.accept(synthesizer);
                    }
                },
                (bytes) -> {
                    final byte[] bytesArray = new byte[bytes.remaining()];
                    bytes.get(bytesArray, 0, bytesArray.length);
                    onPart.accept(bytesArray);
                    log.info("TTSStreamTask: {}: onData {} bytes", idx.incrementAndGet(), bytesArray.length);
                },
                (response)->{
                    onCompleted.accept(true);
                    log.info("TTSStreamTask: gen wav stream cost={} ms", System.currentTimeMillis() - startInMs);
                },
                (response)-> {
                    onCompleted.accept(false);
                    log.warn("tts failed: {}", response);
                });
        task.start();
    }

    private final Supplier<TTSAgent> _getTTSAgent;
    private final Consumer<SpeechSynthesizer> _onSynthesizer;
    private String _key;
    private String _text;
    private String _voice;
    private String _pitchRate;
    private String _speechRate;
    private boolean _cache;
}
