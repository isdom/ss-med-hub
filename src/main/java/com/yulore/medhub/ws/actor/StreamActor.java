package com.yulore.medhub.ws.actor;

import com.yulore.medhub.session.StreamSession;
import com.yulore.medhub.ws.WsHandler;
import org.java_websocket.WebSocket;

import java.nio.ByteBuffer;

public abstract class StreamActor implements WsHandler {
    @Override
    public void onAttached(WebSocket webSocket) {
    }

    @Override
    public void onClose(final WebSocket webSocket) {
        if (_ss != null) {
            _ss.close();
        }
    }

    public StreamSession _ss;
}
