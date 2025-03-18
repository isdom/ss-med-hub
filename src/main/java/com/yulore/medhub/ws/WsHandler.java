package com.yulore.medhub.ws;

import org.java_websocket.WebSocket;

import java.nio.ByteBuffer;

public interface WsHandler {
    void onAttached(WebSocket webSocket);
    void onClose(WebSocket webSocket);
    void onMessage(final WebSocket webSocket, final String message);
    void onMessage(final WebSocket webSocket, final ByteBuffer bytes);

    default void onWebsocketError(Exception ex) {}

    public static final WsHandler DUMMP_HANDLER = new WsHandler() {
        @Override
        public void onAttached(WebSocket webSocket) {
        }

        @Override
        public void onClose(WebSocket webSocket) {
        }

        @Override
        public void onMessage(WebSocket webSocket, String message) {
        }

        @Override
        public void onMessage(WebSocket webSocket, ByteBuffer bytes) {
        }
    };
}
