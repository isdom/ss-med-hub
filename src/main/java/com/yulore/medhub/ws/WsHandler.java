package com.yulore.medhub.ws;

import io.micrometer.core.instrument.Timer;
import org.java_websocket.WebSocket;

import java.nio.ByteBuffer;

public interface WsHandler {
    void onAttached(WebSocket webSocket);
    void onClose(WebSocket webSocket);
    void onMessage(final WebSocket webSocket, final String message, final Timer.Sample sample);

    default void onMessage(final WebSocket webSocket, final ByteBuffer bytes, final long timestampInMs) {}
    default void onWebsocketError(Exception ex) {}
}
