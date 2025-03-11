package com.yulore.bst;


import com.alibaba.nls.client.protocol.tts.SpeechSynthesizer;
import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.mgnt.utils.StringUnicodeEncoderDecoder;
import com.yulore.medhub.nls.TTSAgent;
import com.yulore.medhub.nls.TTSTask;
import com.yulore.util.VarsUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@Slf4j
public class TTSStreamTask implements BuildStreamTask {
    final HashFunction _MD5 = Hashing.md5();

    public TTSStreamTask(final String path, final CompletionStage<TTSAgent> getTTSAgent, final Consumer<SpeechSynthesizer> onSynthesizer) {
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

        final String rawText = VarsUtil.extractValue(vars, "text");
        if (rawText == null) {
            log.warn("{} missing text, ignore", path);
            throw new RuntimeException(path + " missing text.");
        }
        _text = StringUnicodeEncoderDecoder.decodeUnicodeSequenceToString(rawText);

        _voice = VarsUtil.extractValue(vars, "voice");
        _pitch_rate = VarsUtil.extractValue(vars, "pitch_rate");
        _speech_rate = VarsUtil.extractValue(vars, "speech_rate");
        _volume = VarsUtil.extractValue(vars, "volume");
        final boolean _cache = VarsUtil.extractValueAsBoolean(vars, "cache", false);
        _key = _cache ? buildKey() : null;
    }

    private String buildKey() {
        final StringBuilder sb = new StringBuilder();
        sb.append(_voice);
        sb.append(":");
        sb.append(_pitch_rate);
        sb.append(":");
        sb.append(_speech_rate);
        sb.append(":");
        sb.append(_volume);
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

        _getTTSAgent.whenComplete((agent, ex)->{
            if (ex != null) {
                log.error("failed to get tts agent", ex);
                return;
            }
            final TTSTask task = new TTSTask(agent,
                    (synthesizer)->{
                        synthesizer.setText(_text);
                        if (null != _voice && !_voice.isEmpty()) {
                            synthesizer.setVoice(_voice);
                        }
                        if (null != _pitch_rate && !_pitch_rate.isEmpty()) {
                            synthesizer.setPitchRate(Integer.parseInt(_pitch_rate));
                        }
                        if (null != _speech_rate && !_speech_rate.isEmpty()) {
                            synthesizer.setSpeechRate(Integer.parseInt(_speech_rate));
                        }
                        if (null != _volume && !_volume.isEmpty()) {
                            synthesizer.setVolume(Integer.parseInt(_volume));
                        }

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
        });
    }

    private final CompletionStage<TTSAgent> _getTTSAgent;
    private final Consumer<SpeechSynthesizer> _onSynthesizer;
    private final String _key;
    private final String _text;
    private final String _voice;
    private final String _pitch_rate;
    private final String _speech_rate;
    private final String _volume;
}
