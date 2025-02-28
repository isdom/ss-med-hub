package com.yulore.bst;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yulore.medhub.api.CompositeVO;
import com.yulore.util.WaveUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
public class CompositeStreamTask implements BuildStreamTask {
    public CompositeStreamTask(final String path,
                               final Function<CompositeVO, BuildStreamTask> cvo2bst,
                               final boolean removeWavHdr) {
        _cvo2bst = cvo2bst;
        _removeWavHdr = removeWavHdr;
        // eg: rms://{type=cp,url=ws://172.18.86.131:6789/cp,[{"bucket":"ylhz-aicall","object":"aispeech/wxrecoding/100007/f32a59ff70394bf7b1c2fe8455f5b3b1.wav"},
        //     {"type":"tts","voice":"voice-8874311","text":"我这边是美易借钱的,就是之前的国美易卡."},
        //     {"bucket":"ylhz-aicall","object":"aispeech/wxrecoding/100007/2981cf9558f1415f8113cce725700070.wav"}],...}
        final int leftBracePos = path.indexOf('[');
        if (leftBracePos == -1) {
            log.warn("{} missing vars, ignore", path);
            throw new RuntimeException(path + "missing vars, ignore");
        }
        final int rightBracePos = path.indexOf(']', leftBracePos);
        if (rightBracePos == -1) {
            log.warn("{} missing vars, ignore", path);
            throw new RuntimeException(path + "missing vars, ignore");
        }
        final String vars = path.substring(leftBracePos, rightBracePos + 1);
        try {
            final CompositeVO[] cvos = new ObjectMapper()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .readValue(vars, CompositeVO[].class);
            log.info("got cvos: {}", Arrays.toString(cvos));
            _cvos.addAll(Arrays.asList(cvos));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String key() {
        return null;
    }

    @Override
    public void buildStream(final Consumer<byte[]> onPart, final Consumer<Boolean> onCompleted) {
        if (!_removeWavHdr) {
            // first: feed wav header
            onPart.accept(WaveUtil.genWaveHeader(16000, 1));
        }
        // then: feed stream generate by bst one-by-one, previous 's onCompleted then start next generate
        doBuildStream(onPart, onCompleted);
    }

    public void doBuildStream(final Consumer<byte[]> onPart, final Consumer<Boolean> onCompleted) {
        while (!_cvos.isEmpty()) {
            final BuildStreamTask bst = _cvo2bst.apply(_cvos.remove(0));
            if (bst != null) {
                bst.buildStream(onPart, (isOK) -> doBuildStream(onPart, onCompleted));
                return;
            }
        }
        onCompleted.accept(true);
    }

    private final boolean _removeWavHdr;
    private final List<CompositeVO> _cvos = new ArrayList<>();
    private final Function<CompositeVO, BuildStreamTask> _cvo2bst;
}
