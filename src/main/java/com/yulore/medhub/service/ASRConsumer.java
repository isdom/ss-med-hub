package com.yulore.medhub.service;

import com.alibaba.nls.client.protocol.asr.SpeechTranscriber;
import com.tencent.asrv2.SpeechRecognizer;
import com.yulore.medhub.vo.PayloadSentenceBegin;
import com.yulore.medhub.vo.PayloadSentenceEnd;
import com.yulore.medhub.vo.PayloadTranscriptionResultChanged;

public interface ASRConsumer {
    default void onSpeechTranscriberCreated(final SpeechTranscriber speechTranscriber) {}
    default void onSpeechRecognizerCreated(final SpeechRecognizer speechRecognizer) {}

    void onSentenceBegin(final PayloadSentenceBegin payload);
    void onTranscriptionResultChanged(final PayloadTranscriptionResultChanged payload);
    void onSentenceEnd(final PayloadSentenceEnd payload);
    void onTranscriberFail();
}
