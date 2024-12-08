package com.yulore.medhub.session;

import com.alibaba.fastjson.JSON;
import com.mgnt.utils.StringUnicodeEncoderDecoder;
import com.yulore.medhub.api.AIReplyVO;
import com.yulore.medhub.api.ApiResponse;
import com.yulore.medhub.api.ApplySessionVO;
import com.yulore.medhub.api.ScriptApi;
import com.yulore.medhub.vo.HubEventVO;
import com.yulore.medhub.vo.PayloadCallStarted;
import com.yulore.medhub.vo.PayloadSentenceBegin;
import com.yulore.medhub.vo.PayloadSentenceEnd;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@ToString
@Slf4j
public class CallSession extends ASRSession {
    static final long CHECK_IDLE_TIMEOUT = 5000L; // 5 seconds to report check idle to script engine

    public CallSession(final ScriptApi scriptApi, final String bucket) {
        _sessionId = null;
        _scriptApi = scriptApi;
        _bucket = bucket;
    }

    public void notifyUserAnswer(final WebSocket webSocket) {
        try {
            final ApiResponse<ApplySessionVO> response = _scriptApi.apply_session("", "");
            _sessionId = response.getData().getSessionId();
            _welcomeReply = response.getData();
            _callSessions.put(_sessionId, this);
            log.info("apply session response: {}", response);
            HubEventVO.sendEvent(webSocket, "CallStarted", new PayloadCallStarted(response.getData().getSessionId()));
        } catch (Exception ex) {
            log.warn("failed for _scriptApi.apply_session, detail: {}", ex.toString());
        }
    }

    public void checkIdle() {
        final long idleTime = System.currentTimeMillis() - Math.max(_idleStartInMs.get(), _playback != null ? _playback.idleStartInMs() : 0);
        if (_sessionId != null // user answered
            && !_isUserSpeak.get() // user not speak
            && (_playback != null && !_playback.isPlaying())) {
            if (idleTime > CHECK_IDLE_TIMEOUT) {
                log.info("checkIdle: idle duration: {} ms >=: [{}] ms\n", idleTime, CHECK_IDLE_TIMEOUT);
                final ApiResponse<AIReplyVO> response = _scriptApi.ai_reply(_sessionId, null, _isUserSpeak.get() ? 1 : 0, idleTime);
                if (response.getData() != null) {
                    doPlayback(response.getData());
                }
            }
        }
        log.info("checkIdle: sessionId: {}/is_speaking: {}/is_playing: {}/idle duration: {} ms",
                _sessionId, _isUserSpeak.get(), _playback != null && _playback.isPlaying(), idleTime);
    }

    @Override
    public void notifySentenceBegin(final PayloadSentenceBegin payload) {
        super.notifySentenceBegin(payload);
        _isUserSpeak.set(true);
    }

    @Override
    public void notifySentenceEnd(final PayloadSentenceEnd payload) {
        super.notifySentenceEnd(payload);
        _isUserSpeak.set(false);
        _idleStartInMs.set(System.currentTimeMillis());
    }

    @Override
    public void close() {
        super.close();
        _callSessions.remove(_sessionId);
    }

    public void attach(final PlaybackSession playback, final Consumer<String> playbackOn) {
        _playback = playback;
        _playbackOn = playbackOn;
        doPlayback(_welcomeReply);
    }

    private void doPlayback(final AIReplyVO replyVO) {
        log.info("doPlayback: {}", replyVO);
        if ("cp".equals(replyVO.getVoiceMode())) {
            _playbackOn.accept(String.format("type=cp,%s", JSON.toJSONString(replyVO.getCps())));
        } else if ("wav".equals(replyVO.getVoiceMode())) {
            _playbackOn.accept(String.format("{bucket=%s}%s", _bucket, replyVO.getAi_speech_file()));
        } else if ("tts".equals(replyVO.getVoiceMode())) {
            _playbackOn.accept(String.format("{type=tts,text=%s}tts.wav",
                    StringUnicodeEncoderDecoder.encodeStringToUnicodeSequence(replyVO.getReply_content())));
        } else {
            log.info("doPlayback: unknown reply: {}, ignore", replyVO);
        }
    }

    static public CallSession findBy(final String sessionId) {
        return _callSessions.get(sessionId);
    }

    private final ScriptApi _scriptApi;
    private final String _bucket;
    private String _sessionId;
    private AIReplyVO _welcomeReply;
    private Consumer<String> _playbackOn;
    private PlaybackSession _playback;
    private AtomicLong _idleStartInMs = new AtomicLong(System.currentTimeMillis());
    private AtomicBoolean _isUserSpeak = new AtomicBoolean(false);

    static final ConcurrentMap<String, CallSession> _callSessions = new ConcurrentHashMap<>();
}
