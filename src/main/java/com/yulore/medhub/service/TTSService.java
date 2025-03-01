package com.yulore.medhub.service;

import com.yulore.medhub.nls.ASRAgent;
import com.yulore.medhub.nls.CosyAgent;
import com.yulore.medhub.nls.TTSAgent;
import com.yulore.medhub.nls.TxASRAgent;
import com.yulore.medhub.vo.WSCommandVO;
import org.java_websocket.WebSocket;

public interface TTSService {
    TTSAgent selectTTSAgent();
    CosyAgent selectCosyAgent();
}
