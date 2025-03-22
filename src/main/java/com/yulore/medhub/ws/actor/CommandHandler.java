package com.yulore.medhub.ws.actor;

import com.yulore.medhub.vo.WSCommandVO;
import com.yulore.medhub.ws.WSCommandRegistry;
import com.yulore.medhub.ws.WsHandler;
import com.yulore.util.ExceptionUtil;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;

@Slf4j
abstract class CommandHandler<ACTOR extends CommandHandler<?>> implements WsHandler {
    @Override
    public void onMessage(final WebSocket webSocket, final String message, final Timer.Sample sample) {
        try {
            commandRegistry().handleCommand(WSCommandVO.parse(message, WSCommandVO.WSCMD_VOID), message, (ACTOR)this, webSocket, sample);
        } catch (Exception ex) {
            log.error("handleCommand {}: {}, an error occurred: {}",
                    webSocket.getRemoteSocketAddress(), message, ExceptionUtil.exception2detail(ex));
        }
    }

    protected abstract WSCommandRegistry<ACTOR> commandRegistry();
}
