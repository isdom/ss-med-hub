package com.yulore.medhub.ws;

import org.java_websocket.WebSocket;

import java.nio.ByteBuffer;

public interface WsHandler {
    void onAttached(WebSocket webSocket);
    void onClose(WebSocket webSocket);
    void onMessage(final WebSocket webSocket, final String message);
    void onMessage(final WebSocket webSocket, final ByteBuffer bytes);
}
