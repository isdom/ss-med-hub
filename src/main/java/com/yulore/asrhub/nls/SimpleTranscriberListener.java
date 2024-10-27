package com.yulore.asrhub.nls;

import com.alibaba.nls.client.protocol.asr.SpeechTranscriberListener;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriberResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Consumer;

@Slf4j
public class SimpleTranscriberListener extends SpeechTranscriberListener {
    final Consumer<SpeechTranscriberResponse> onSentenceEnd;

    static public SpeechTranscriberListener getTranscriberListener(final Consumer<SpeechTranscriberResponse> onSentenceEnd) {
        return new SimpleTranscriberListener(onSentenceEnd);
    }

    SimpleTranscriberListener(final Consumer<SpeechTranscriberResponse> onSentenceEnd) {
        this.onSentenceEnd = onSentenceEnd;
    }

    //识别出中间结果。仅当setEnableIntermediateResult为true时，才会返回该消息。
    @Override
    public void onTranscriptionResultChange(final SpeechTranscriberResponse response) {
        log.info("task_id: {}, name: {}, status: {}, index: {}, result: {}, time: {}",
                response.getTaskId(),
                response.getName(),
                response.getStatus(),
                response.getTransSentenceIndex(),
                response.getTransSentenceText(),
                response.getTransSentenceTime());
    }

    @Override
    public void onTranscriberStart(final SpeechTranscriberResponse response) {
        //task_id是调用方和服务端通信的唯一标识，遇到问题时，需要提供此task_id。
        log.info("task_id: {}, name: {}, status: {}",
                response.getTaskId(),
                response.getName(),
                response.getStatus());
    }

    @Override
    public void onSentenceBegin(SpeechTranscriberResponse response) {
        log.info("task_id: {}, name: {}, status: {}", response.getTaskId(), response.getName(), response.getStatus());

    }

    //识别出一句话。服务端会智能断句，当识别到一句话结束时会返回此消息。
    @Override
    public void onSentenceEnd(final SpeechTranscriberResponse response) {
        log.info("task_id: {}, name: {}, status: {}, index: {}, result: {}, confidence: {}, begin_time: {}, time: {}",
                response.getTaskId(),
                response.getName(),
                response.getStatus(),
                response.getTransSentenceIndex(),
                response.getTransSentenceText(),
                response.getConfidence(),
                response.getSentenceBeginTime(),
                response.getTransSentenceTime());
        onSentenceEnd.accept(response);
    }

    //识别完毕
    @Override
    public void onTranscriptionComplete(final SpeechTranscriberResponse response) {
        log.info("task_id: {}, name: {}, status: {}", response.getTaskId(), response.getName(), response.getStatus());
    }

    @Override
    public void onFail(final SpeechTranscriberResponse response) {
        //task_id是调用方和服务端通信的唯一标识，遇到问题时，需要提供此task_id。
        log.warn("task_id: {}, status: {}, status_text: {}",
                response.getTaskId(),
                response.getStatus(),
                response.getStatusText());
    }
}
