package com.yulore.medhub.vo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;

@Data
@ToString
@Slf4j
public class HubEventVO<PAYLOAD> {
    @Data
    @ToString
    public static class Header {
        String name;
    }
    Header header;
    PAYLOAD payload;

    public static <PAYLOAD> void sendEvent(final WebSocket webSocket, final String eventName, final PAYLOAD payload) {
        final HubEventVO<PAYLOAD> event = new HubEventVO<>();
        final HubEventVO.Header header = new HubEventVO.Header();
        header.setName(eventName);
        event.setHeader(header);
        event.setPayload(payload);
        try {
            webSocket.send(new ObjectMapper().writeValueAsString(event));
        } catch (JsonProcessingException ex) {
            log.warn("sendEvent {}: {}, an error occurred when parseAsJson: {}",
                    webSocket.getRemoteSocketAddress(), event, ex.toString());
        }
    }
}
