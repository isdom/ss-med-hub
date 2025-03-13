package com.yulore.medhub.service;

import com.yulore.medhub.vo.WSCommandVO;
import com.yulore.medhub.vo.cmd.VOStartTranscription;
import org.java_websocket.WebSocket;

public interface ASRService {
    void startTranscription(final VOStartTranscription vo, final WebSocket webSocket);
    void stopTranscription(final WebSocket webSocket);
}
