package com.yulore.medhub.ws;

import lombok.extern.slf4j.Slf4j;
import org.java_websocket.server.WebSocketServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class WsBootstrap {

    @Autowired
    WebSocketServer _ws;
}
