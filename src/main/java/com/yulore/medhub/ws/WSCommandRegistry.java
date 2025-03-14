package com.yulore.medhub.ws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.yulore.medhub.vo.WSCommandVO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.util.Pair;
import org.java_websocket.WebSocket;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

@Slf4j
public class WSCommandRegistry<ACTOR> {

    public record CommandContext<ACTOR, PAYLOAD>(ACTOR actor, PAYLOAD payload, WebSocket ws) {
    }

    public <PAYLOAD> WSCommandRegistry<ACTOR> register(final TypeReference<WSCommandVO<PAYLOAD>> type,
                                                       final String name,
                                                       final Consumer<CommandContext<ACTOR, PAYLOAD>> handler) {
        _command2handler.put(name, (Pair)Pair.of(type, handler));
        return this;
    }

    public void handleCommand(final WSCommandVO<Void> cmd, final String message, final ACTOR actor, final WebSocket ws) throws JsonProcessingException {
        final Pair<?,?> pair = _command2handler.get(cmd.getHeader().get("name"));
        if (pair != null) {
            final Consumer<CommandContext<?,?>> consumer = (Consumer<CommandContext<?,?>>)pair.getSecond();
            final TypeReference<WSCommandVO<Object>> type = (TypeReference<WSCommandVO<Object>>)pair.getFirst();
            consumer.accept(new CommandContext<>(actor, WSCommandVO.parse(message, type).payload, ws));
        } else {
            log.warn("handleCommand: Unknown Command: {}", cmd);
        }
    }

    private final Map<String, Pair<TypeReference<WSCommandVO<?>>, Consumer<CommandContext<?,?>>>> _command2handler = new HashMap<>();
}
