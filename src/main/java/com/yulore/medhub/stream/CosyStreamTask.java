package com.yulore.medhub.stream;

import com.alibaba.nls.client.protocol.OutputFormatEnum;
import com.alibaba.nls.client.protocol.SampleRateEnum;
import com.alibaba.nls.client.protocol.tts.StreamInputTts;
import com.alibaba.nls.client.protocol.tts.StreamInputTtsListener;
import com.alibaba.nls.client.protocol.tts.StreamInputTtsResponse;
import com.mgnt.utils.StringUnicodeEncoderDecoder;
import com.yulore.medhub.nls.CosyAgent;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@Slf4j
public class CosyStreamTask implements BuildStreamTask {
    public CosyStreamTask(final String path, final CosyAgent agent) {
        _agent = agent;
        // eg: {type=cosy,voice=xxx,url=ws://172.18.86.131:6789/cosy,vars_playback_id=<uuid>,content_id=2088788,vars_start_timestamp=1732028219711854}
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
    public void buildStream(final Consumer<byte[]> onPart, final Consumer<Boolean> onCompleted) {
        log.info("start gen cosyvoice: {}", _text);
        final AtomicInteger idx = new AtomicInteger(0);
        final long startInMs = System.currentTimeMillis();
        final StreamInputTtsListener listener =  new StreamInputTtsListener() {
            //流入语音合成开始
            @Override
            public void onSynthesisStart(final StreamInputTtsResponse response) {
                log.info("onSynthesisStart: name: {} , status: {}", response.getName(), response.getStatus());
            }

            //服务端检测到了一句话的开始
            @Override
            public void onSentenceBegin(final StreamInputTtsResponse response) {
                log.info("onSentenceBegin: name: {} , status: {}", response.getName(), response.getStatus());
            }

            //服务端检测到了一句话的结束，获得这句话的起止位置和所有时间戳
            @Override
            public void onSentenceEnd(final StreamInputTtsResponse response) {
                log.info("onSentenceEnd: name: {} , status: {}， subtitles: {}",
                        response.getName(), response.getStatus(), response.getObject("subtitles"));
            }

            //流入语音合成结束
            @Override
            public void onSynthesisComplete(final StreamInputTtsResponse response) {
                // 调用onSynthesisComplete时，表示所有TTS数据已经接收完成，所有文本都已经合成音频并返回
                log.info("onSynthesisComplete: name: {} , status: {}", response.getName(), response.getStatus());
                onCompleted.accept(true);
                log.info("CosyStreamTask: gen wav stream cost={} ms", System.currentTimeMillis() - startInMs);
            }

            //收到语音合成的语音二进制数据
            @Override
            public void onAudioData(final ByteBuffer message) {
                byte[] bytesArray = new byte[message.remaining()];
                message.get(bytesArray, 0, bytesArray.length);
                log.info("CosyStreamTask: {}: onData {} bytes", idx.incrementAndGet(), bytesArray.length);
                onPart.accept(bytesArray);
            }

            //收到语音合成的增量音频时间戳
            @Override
            public void onSentenceSynthesis(final StreamInputTtsResponse response) {
                log.info("onSentenceSynthesis: name: {}, status: {}, subtitles: {}",
                        response.getName(), response.getStatus(), response.getObject("subtitles"));
            }

            @Override
            public void onFail(final StreamInputTtsResponse response) {
                // task_id是调用方和服务端通信的唯一标识，当遇到问题时，需要提供此task_id以便排查。
                log.info("session_id: {}, task_id: {}, status: {}, status_text: {}",
                        getStreamInputTts().getCurrentSessionId(), response.getTaskId(), response.getStatus(), response.getStatusText());
                onCompleted.accept(false);
            }
        };

        StreamInputTts synthesizer = null;
        try {
            synthesizer = _agent.buildCosyvoiceSynthesizer(listener);
            if (null != _voice) {
                synthesizer.setVoice(_voice);
            }
            if (null != _pitchRate) {
                synthesizer.setPitchRate(Integer.parseInt(_pitchRate));
            }
            if (null != _speechRate) {
                synthesizer.setSpeechRate(Integer.parseInt(_speechRate));
            }
            //音量，范围是0~100，可选，默认50。
            synthesizer.setVolume(100);

            synthesizer.setFormat(OutputFormatEnum.WAV);
            //设置返回音频的采样率。
            synthesizer.setSampleRate(SampleRateEnum.SAMPLE_RATE_16K);
            synthesizer.startStreamInputTts();
            _agent.incConnected();
            synthesizer.setMinSendIntervalMS(100);
            synthesizer.sendStreamInputTts(_text);
            //通知服务端流入文本数据发送完毕，阻塞等待服务端处理完成。
            synthesizer.stopStreamInputTts();
        } catch (Exception ex) {
            log.warn("buildStream failed: {}", ex.toString());
        } finally {
            //关闭连接
            if (null != synthesizer) {
                synthesizer.close();
            }
            _agent.decConnected();
            _agent.decConnection();
        }
    }

    private final CosyAgent _agent;
    // private String _key;
    private String _text;
    private String _voice;
    private String _pitchRate;
    private String _speechRate;
}
