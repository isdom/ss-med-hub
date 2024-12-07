package com.yulore.medhub.session;

import com.alibaba.fastjson.JSON;
import com.yulore.medhub.api.AIReplyVO;
import com.yulore.medhub.api.ApiResponse;
import com.yulore.medhub.api.ApplySessionVO;
import com.yulore.medhub.api.ScriptApi;
import com.yulore.medhub.vo.HubEventVO;
import com.yulore.medhub.vo.PayloadCallStarted;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;

import java.text.Format;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

@ToString
@Slf4j
public class CallSession extends ASRSession {
    public CallSession(final ScriptApi scriptApi) {
        _scriptApi = scriptApi;
    }

    public void notifyUserAnswer(final WebSocket webSocket) {
        try {
            final ApiResponse<ApplySessionVO> response = _scriptApi.apply_session("", "");
            _sessionId = response.getData().getSessionId();
            _currentReply = response.getData();
            _callSessions.put(_sessionId, this);
            log.info("apply session response: {}", response);
            HubEventVO.sendEvent(webSocket, "CallStarted", new PayloadCallStarted(response.getData().getSessionId()));
        } catch (Exception ex) {
            log.warn("failed for _scriptApi.apply_session, detail: {}", ex.toString());
        }
    }

    public void close() {
        super.close();
        _callSessions.remove(_sessionId);
    }

    public void attach(final PlaybackSession playback, final Consumer<String> playbackOn) {
        _playback = playback;
        if ("cp".equals(_currentReply.getVoiceMode())) {
            playbackOn.accept(String.format("type=cp,%s", JSON.toJSONString(_currentReply.getCps())));
        }
    }

    static public CallSession findBy(final String sessionId) {
        return _callSessions.get(sessionId);
    }

    private final ScriptApi _scriptApi;
    private String _sessionId;
    private AIReplyVO _currentReply;
    private PlaybackSession _playback;

    static final ConcurrentMap<String, CallSession> _callSessions = new ConcurrentHashMap<>();
}
