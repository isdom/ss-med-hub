package com.yulore.medhub.service;

import com.alibaba.nls.client.protocol.OutputFormatEnum;
import com.alibaba.nls.client.protocol.SampleRateEnum;
import com.aliyun.oss.OSS;
import com.mgnt.utils.StringUnicodeEncoderDecoder;
import com.yulore.bst.*;
import com.yulore.medhub.api.CompositeVO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service
@ConditionalOnProperty(prefix = "nls", name = "tts-enabled", havingValue = "true")
public class BSTServiceImpl implements BSTService {
    @Override
    public BuildStreamTask getTaskOf(final String path, final boolean removeWavHdr, final int sampleRate) {
        try {
            if (path.contains("type=cp")) {
                return new CompositeStreamTask(path, (cvo) -> {
                    final BuildStreamTask bst = cvo2bst(cvo);
                    if (bst != null) {
                        return bst.key() != null ? _scsService.asCache(bst) : bst;
                    }
                    return null;
                }, removeWavHdr);
            } else if (path.contains("type=tts")) {
                final BuildStreamTask bst = new TTSStreamTask(path, ttsService.selectTTSAgentAsync(), (synthesizer) -> {
                    synthesizer.setFormat(removeWavHdr ? OutputFormatEnum.PCM : OutputFormatEnum.WAV);
                    synthesizer.setSampleRate(sampleRate);
                });
                return bst.key() != null ? _scsService.asCache(bst) : bst;
            } else if (path.contains("type=cosy")) {
                final BuildStreamTask bst = new CosyStreamTask(path, ttsService.selectCosyAgentAsync(), (synthesizer) -> {
                    synthesizer.setFormat(removeWavHdr ? OutputFormatEnum.PCM : OutputFormatEnum.WAV);
                    synthesizer.setSampleRate(sampleRate);
                });
                return bst.key() != null ? _scsService.asCache(bst) : bst;
            } else {
                final BuildStreamTask bst = new OSSStreamTask(path, _ossProvider.getObject(), removeWavHdr);
                return bst.key() != null ? _scsService.asCache(bst) : bst;
            }
        } catch (Exception ex) {
            log.warn("getTaskOf failed: {}", ex.toString());
            return null;
        }
    }

    private BuildStreamTask cvo2bst(final CompositeVO cvo) {
        if (cvo.getBucket() != null && !cvo.getBucket().isEmpty() && cvo.getObject() != null && !cvo.getObject().isEmpty()) {
            log.info("support CVO => OSS Stream: {}", cvo);
            return new OSSStreamTask(
                    "{bucket=" + cvo.bucket + ",cache=" + cvo.cache + ",start=" + cvo.start + ",end=" + cvo.end + "}" + cvo.object,
                    _ossProvider.getObject(), true);
        } else if (cvo.getType() != null && cvo.getType().equals("tts")) {
            log.info("support CVO => TTS Stream: {}", cvo);
            return genTtsStreamTask(cvo);
        } else if (cvo.getType() != null && cvo.getType().equals("cosy")) {
            log.info("support CVO => Cosy Stream: {}", cvo);
            return genCosyStreamTask(cvo);
        } else {
            log.info("not support cvo: {}, skip", cvo);
            return null;
        }
    }

    private BuildStreamTask genCosyStreamTask(final CompositeVO cvo) {
        return new CosyStreamTask(cvo2cosy(cvo), ttsService.selectCosyAgentAsync(), (synthesizer) -> {
            //设置返回音频的编码格式
            synthesizer.setFormat(OutputFormatEnum.PCM);
            //设置返回音频的采样率。
            synthesizer.setSampleRate(SampleRateEnum.SAMPLE_RATE_16K);
        });
    }

    private BuildStreamTask genTtsStreamTask(final CompositeVO cvo) {
        return new TTSStreamTask(cvo2tts(cvo), ttsService.selectTTSAgentAsync(), (synthesizer) -> {
            //设置返回音频的编码格式
            synthesizer.setFormat(OutputFormatEnum.PCM);
            //设置返回音频的采样率
            synthesizer.setSampleRate(SampleRateEnum.SAMPLE_RATE_16K);
        });
    }

    static private String cvo2tts(final CompositeVO cvo) {
        // {type=tts,voice=xxx,url=ws://172.18.86.131:6789/playback,vars_playback_id=<uuid>,content_id=2088788,vars_start_timestamp=1732028219711854,text='StringUnicodeEncoderDecoder.encodeStringToUnicodeSequence(content)'}
        //          unused.wav
        return String.format("{type=tts,cache=%s,voice=%s,pitch_rate=%s,speech_rate=%s,volume=%s,text=%s}tts.wav",
                cvo.cache,
                cvo.voice,
                cvo.pitch_rate,
                cvo.speech_rate,
                cvo.volume,
                StringUnicodeEncoderDecoder.encodeStringToUnicodeSequence(cvo.text));
    }

    static private String cvo2cosy(final CompositeVO cvo) {
        // eg: {type=cosy,voice=xxx,url=ws://172.18.86.131:6789/cosy,vars_playback_id=<uuid>,content_id=2088788,vars_start_timestamp=1732028219711854,text='StringUnicodeEncoderDecoder.encodeStringToUnicodeSequence(content)'}
        //          unused.wav
        return String.format("{type=cosy,cache=%s,voice=%s,pitch_rate=%s,speech_rate=%s,volume=%s,text=%s}cosy.wav",
                cvo.cache,
                cvo.voice,
                cvo.pitch_rate,
                cvo.speech_rate,
                cvo.volume,
                StringUnicodeEncoderDecoder.encodeStringToUnicodeSequence(cvo.text));
    }

    @Autowired
    private StreamCacheService _scsService;

    private final ObjectProvider<OSS> _ossProvider;

    @Autowired
    private TTSService ttsService;
}
