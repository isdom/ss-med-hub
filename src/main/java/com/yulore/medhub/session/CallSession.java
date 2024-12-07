package com.yulore.medhub.session;

import com.yulore.medhub.api.ApiResponse;
import com.yulore.medhub.api.ApplySessionVO;
import com.yulore.medhub.api.ScriptApi;
import com.yulore.medhub.vo.HubEventVO;
import com.yulore.medhub.vo.PayloadCallStarted;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;

import java.util.UUID;

@ToString
@Slf4j
public class CallSession extends ASRSession {
    public CallSession(final ScriptApi scriptApi) {
        _scriptApi = scriptApi;
    }

    public void notifyUserAnswer(final WebSocket webSocket) {
        try {
            final ApiResponse<ApplySessionVO> response = _scriptApi.apply_session("", "");
            log.info("apply session response: {}", response);
            HubEventVO.sendEvent(webSocket, "CallStarted", new PayloadCallStarted(response.getData().getSessionId()));
        } catch (Exception ex) {
            log.warn("failed for _scriptApi.apply_session, detail: {}", ex.toString());
        }
    }

    private final ScriptApi _scriptApi;
}
