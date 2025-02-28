package com.yulore.medhub.ws;

import org.java_websocket.WebSocket;

public interface WsHandler {
    void onAttached(WebSocket webSocket);
}
