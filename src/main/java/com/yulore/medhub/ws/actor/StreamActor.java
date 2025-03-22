package com.yulore.medhub.ws.actor;

import com.yulore.medhub.session.StreamSession;
import org.java_websocket.WebSocket;

public abstract class StreamActor extends CommandHandler<StreamActor> {
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
