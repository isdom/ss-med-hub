package com.yulore.medhub.service;

import com.yulore.medhub.nls.ASRAgent;
import com.yulore.medhub.nls.CosyAgent;
import com.yulore.medhub.nls.TTSAgent;
import com.yulore.medhub.nls.TxASRAgent;
import com.yulore.medhub.vo.WSCommandVO;
import org.java_websocket.WebSocket;

public interface NlsService {
    ASRAgent selectASRAgent();
    TTSAgent selectTTSAgent();
    CosyAgent selectCosyAgent();
    TxASRAgent selectTxASRAgent();
    void startTranscription(final WSCommandVO cmd, final WebSocket webSocket);
    void stopTranscription(final WSCommandVO cmd, final WebSocket webSocket);
}
