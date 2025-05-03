package com.yulore.medhub.service;

import com.yulore.medhub.vo.PayloadSentenceBegin;
import com.yulore.medhub.vo.PayloadSentenceEnd;
import com.yulore.medhub.vo.PayloadTranscriptionResultChanged;

public interface ASRConsumer {
    void onSentenceBegin(final PayloadSentenceBegin payload);
    void onTranscriptionResultChanged(final PayloadTranscriptionResultChanged payload);
    void onSentenceEnd(final PayloadSentenceEnd payload);
    void onTranscriberFail();
}
