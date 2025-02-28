package com.yulore.medhub.ws.actor;

import com.yulore.medhub.ws.WsHandler;
import org.java_websocket.WebSocket;

import java.nio.ByteBuffer;

public abstract class StreamActor implements WsHandler {
    @Override
    public void onAttached(WebSocket webSocket) {

    }

    @Override
    public void onClose(WebSocket webSocket) {

    }
}
