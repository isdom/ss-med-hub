package com.yulore.medhub.nls;

import com.alibaba.nls.client.protocol.tts.SpeechSynthesizer;
import com.alibaba.nls.client.protocol.tts.SpeechSynthesizerListener;
import com.alibaba.nls.client.protocol.tts.SpeechSynthesizerResponse;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

@Slf4j
public class TTSTask {
    private SpeechSynthesizer _synthesizer = null;

    public TTSTask(final TTSAgent agent,
                   final Consumer<SpeechSynthesizer> setupSynthesizer,
                   final Consumer<ByteBuffer> onData,
                   final Consumer<SpeechSynthesizerResponse> onComplete,
                   final Consumer<SpeechSynthesizerResponse> onFail
                   ) {
        try {
            _synthesizer = agent.buildSpeechSynthesizer(new SpeechSynthesizerListener() {
                //语音合成结束
                @Override
                public void onComplete(final SpeechSynthesizerResponse response) {
                    //调用onComplete时表示所有TTS数据已接收完成，因此为整个合成数据的延迟。该延迟可能较大，不一定满足实时场景。
                    log.info("onComplete: name:{}, status:{}", response.getName(), response.getStatus());
                    _synthesizer.close();
                    agent.decConnectionAsync();
                    onComplete.accept(response);
                }

                //语音合成的语音二进制数据
                @Override
                public void onMessage(final ByteBuffer bytes) {
                    onData.accept(bytes);
                }

                @Override
                public void onFail(final SpeechSynthesizerResponse response){
                    //task_id是调用方和服务端通信的唯一标识，当遇到问题时需要提供task_id以便排查。
                    log.info("onFail: task_id:{}, status:{}, status_text:{}",
                            response.getTaskId(), response.getStatus(), response.getStatusText());
                    agent.decConnectionAsync();
                    onFail.accept(response);
                }
            });
            //发音人
            // synthesizer.setVoice("siyue");
            //语调，范围是-500~500，可选，默认是0。
            //synthesizer.setPitchRate(100);
            //语速，范围是-500~500，默认是0。
            //synthesizer.setSpeechRate(100);
            //设置用于语音合成的文本
            // 是否开启字幕功能（返回相应文本的时间戳），默认不开启，需要注意并非所有发音人都支持该参数。
            // synthesizer.addCustomedParam("enable_subtitle", false);
            setupSynthesizer.accept(_synthesizer);
        } catch (Exception ex) {
            log.warn("failed to launch tts task, detail: {}", ex.toString());
        }
    }

    public boolean start() {
        try {
            //此方法将以上参数设置序列化为JSON格式发送给服务端，并等待服务端确认。
            _synthesizer.start();
            return true;
        } catch (Exception ex) {
            log.warn("failed to start tts task, detail: {}", ex.toString());
            return false;
        }
    }

    public void waitForComplete() throws Exception {
        _synthesizer.waitForComplete();
    }
}
