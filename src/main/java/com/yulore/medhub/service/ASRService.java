package com.yulore.medhub.service;

import com.yulore.medhub.vo.WSCommandVO;
import org.java_websocket.WebSocket;

public interface ASRService {
    // ASRAgent selectASRAgent();
    // TxASRAgent selectTxASRAgent();
    void startTranscription(final WSCommandVO cmd, final WebSocket webSocket);
    void stopTranscription(final WSCommandVO cmd, final WebSocket webSocket);
}
