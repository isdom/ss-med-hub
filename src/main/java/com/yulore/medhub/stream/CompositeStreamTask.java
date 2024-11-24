package com.yulore.medhub.stream;


import com.aliyun.oss.OSS;
import com.aliyun.oss.model.OSSObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yulore.medhub.cache.StreamCacheService;
import com.yulore.medhub.vo.HubCommandVO;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.Consumer;

@Slf4j
public class CompositeStreamTask implements BuildStreamTask {
    @Data
    @ToString
    static public class CVO {
        // type
        String t;
        String b;
        String p;
        String v;
        String x;
    }

    public CompositeStreamTask(final String path, final OSS ossClient, final StreamCacheService lssService) {
        _ossClient = ossClient;
        _lssService = lssService;
        // eg: rms://{type=cp,url=ws://172.18.86.131:6789/cp,[{"b":"ylhz-aicall","p":"aispeech/wxrecoding/100007/f32a59ff70394bf7b1c2fe8455f5b3b1.wav"},
        //     {"t":"tts","v":"voice-8874311","x":"我这边是美易借钱的,就是之前的国美易卡."},
        //     {"b":"ylhz-aicall","p":"aispeech/wxrecoding/100007/2981cf9558f1415f8113cce725700070.wav"}],...}
        final int leftBracePos = path.indexOf('[');
        if (leftBracePos == -1) {
            log.warn("{} missing vars, ignore", path);
            throw new RuntimeException(path + "missing vars, ignore");
        }
        final int rightBracePos = path.indexOf('}');
        if (rightBracePos == -1) {
            log.warn("{} missing vars, ignore", path);
            throw new RuntimeException(path + "missing vars, ignore");
        }
        final String vars = path.substring(leftBracePos, rightBracePos + 1);
        try {
            final CVO[] cvos = new ObjectMapper().readValue(vars, CVO[].class);
            log.info("got cvos: {}", Arrays.toString(cvos));
            throw new RuntimeException(Arrays.toString(cvos));
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
    }

    private final OSS _ossClient;
    private final StreamCacheService _lssService;
}
