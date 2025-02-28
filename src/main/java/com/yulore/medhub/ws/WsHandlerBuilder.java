package com.yulore.medhub.ws;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;

@FunctionalInterface
public interface WsHandlerBuilder {
    WsHandler build(final String prefix, final WebSocket webSocket, final ClientHandshake handshake);
}
