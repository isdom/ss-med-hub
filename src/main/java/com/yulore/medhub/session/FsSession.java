package com.yulore.medhub.session;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mgnt.utils.StringUnicodeEncoderDecoder;
import com.yulore.medhub.api.AIReplyVO;
import com.yulore.medhub.api.ApiResponse;
import com.yulore.medhub.api.ScriptApi;
import com.yulore.medhub.vo.PayloadFSPlayback;
import com.yulore.medhub.vo.PayloadSentenceBegin;
import com.yulore.medhub.vo.PayloadSentenceEnd;
import com.yulore.medhub.vo.PayloadTranscriptionResultChanged;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@ToString
@Slf4j
public class FsSession extends ASRSession {
    final static String PLAYBACK_ID_NAME="vars_playback_id";

    public FsSession(final String sessionId,
                     final BiConsumer<String, Object> sendEvent,
                     final ScriptApi scriptApi,
                     final String welcome,
                     final String rms_cp_prefix,
                     final String rms_tts_prefix,
                     final String rms_wav_prefix,
                     final boolean testEnableDelay,
                     final long testDelayMs,
                     final boolean testEnableDisconnect,
                     final float testDisconnectProbability,
                     final Runnable doDisconnect) {
        _sendEvent = sendEvent;
        _scriptApi = scriptApi;
        _welcome = welcome;
        _rms_cp_prefix = rms_cp_prefix;
        _rms_tts_prefix = rms_tts_prefix;
        _rms_wav_prefix = rms_wav_prefix;
        if (testEnableDelay) {
            _delayExecutor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("delayExecutor"));
            _testDelayMs = testDelayMs;
        }
        if (testEnableDisconnect && Math.random() < testDisconnectProbability) {
            _testDisconnectTimeout = (long)(Math.random() * 5000) + 5000L; // 5s ~ 10s
            log.info("[{}]: enable disconnect test feature, timeout: {} ms", sessionId, _testDisconnectTimeout);
        }
        _doDisconnect = doDisconnect;
        _sessionId = sessionId;
    }

    @Override
    public boolean transmit(final ByteBuffer bytes) {
        if ( !_isTranscriptionStarted.get() || _isTranscriptionFailed.get()) {
            return false;
        }

        final Consumer<ByteBuffer> transmitter = _transmitData.get();

        if (transmitter != null) {
            if (_delayExecutor == null) {
                transmitter.accept(bytes);
            } else {
                _delayExecutor.schedule(()->transmitter.accept(bytes), _testDelayMs, TimeUnit.MILLISECONDS);
            }
            _transmitCount.incrementAndGet();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void close() {
        if (_delayExecutor != null) {
            _delayExecutor.shutdownNow();
        }
        super.close();
    }

    public void checkIdle() {
        /*
        final long idleTime = System.currentTimeMillis() - Math.max(_idleStartInMs.get(), _playback.get() != null ? _playback.get().idleStartInMs() : 0);
        boolean isAiSpeaking = _playback.get() != null && _playback.get().isPlaying();
        if (_sessionId != null      // user answered
                && _playback.get() != null
                && !_isUserSpeak.get()  // user not speak
                && !isAiSpeaking        // AI not speak
                && _aiSetting != null
        ) {
            if (idleTime > _aiSetting.getIdle_timeout()) {
                log.info("[{}]: checkIdle: idle duration: {} ms >=: [{}] ms\n", _sessionId, idleTime, CHECK_IDLE_TIMEOUT);
                try {
                    final ApiResponse<AIReplyVO> response =
                            _scriptApi.ai_reply(_sessionId, null, idleTime, 0);
                    if (response.getData() != null) {
                        if (doPlayback(response.getData())) {
                            _lastReply = response.getData();
                        }
                    } else {
                        log.info("[{}]: checkIdle: ai_reply {}, do nothing\n", _sessionId, response);
                    }
                } catch (Exception ex) {
                    log.warn("[{}]: checkIdle: ai_reply error, detail: {}", _sessionId, ex.toString());
                }
            }
        }
        log.info("[{}]: checkIdle: is_speaking: {}/is_playing: {}/idle duration: {} ms",
                _sessionId, _isUserSpeak.get(), isAiSpeaking, idleTime);
         */
    }

    @Override
    public void transcriptionStarted() {
        super.transcriptionStarted();

        try {
            final ApiResponse<AIReplyVO> response =
                    _scriptApi.ai_reply(_sessionId, _welcome, null,0);
            if (response.getData() != null) {
                if (doPlayback(response.getData())) {
                    _lastReply = response.getData();
                }
            } else {
                log.info("[{}]: transcriptionStarted: ai_reply {}, do nothing\n", _sessionId, response);
            }
        } catch (Exception ex) {
            log.warn("[{}]: transcriptionStarted: ai_reply error, detail: {}", _sessionId, ex.toString());
        }
    }

    private boolean doPlayback(final AIReplyVO vo) {
        _currentAIContentId.set(Long.toString(vo.getAi_content_id()));
        if ("cp".equals(vo.getVoiceMode())) {
            playRms(_rms_cp_prefix.replace("{cpvars}", tryExtractCVOS(vo)));
            return true;
        }

        if ("tts".equals(vo.getVoiceMode())) {
            playTTS(vo.getReply_content(), vo.getTts_fixed_content());
            return true;
        }

        if ("wav".equals(vo.getVoiceMode())) {
            playAudio(vo.getAi_speech_file(), vo.getReply_content());
            return true;
        }

        return false;
    }

    private String tryExtractCVOS(final AIReplyVO vo) {
        try {
            return new ObjectMapper().writeValueAsString(vo.getCps());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String playRms(final String cpprefix) {
        final String playback_id = UUID.randomUUID().toString();
        // 更新当前 的 播放ID
        _currentPlaybackId.set(playback_id);

        final String prefix = cpprefix
                //.replace("{uuid}", _uuid)
                .replace("{vars}", String.format("%s=%s,", PLAYBACK_ID_NAME, playback_id)+"%s")
                ;
        final String file = prefix + playback_id + ".wav";

        _sendEvent.accept("FSPlayback", new PayloadFSPlayback(file));
        log.info("[{}]: rms: play [{}] as {}", _sessionId, file, playback_id);

        return playback_id;
    }

    public String playTTS(final String content, Boolean isFixed) {
        final String playback_id = UUID.randomUUID().toString();
        // 更新当前 的 播放ID
        _currentPlaybackId.set(playback_id);

        final String prefix = _rms_tts_prefix
                //.replace("{uuid}", _uuid)
                .replace("{vars}",String.format("%s=%s,text=%s,", PLAYBACK_ID_NAME, playback_id, StringUnicodeEncoderDecoder.encodeStringToUnicodeSequence(content))+"%s");
        final String file = prefix + playback_id + ".wav";

        _sendEvent.accept("FSPlayback", new PayloadFSPlayback(file));
        log.info("[{}]: rms: play [{}] as {}", _sessionId, file, playback_id);
        return playback_id;
    }

    public String playAudio(final String path, final String content) {
        final String playback_id = UUID.randomUUID().toString();
        // 更新当前 的 播放ID
        _currentPlaybackId.set(playback_id);

        final String prefix = _rms_wav_prefix
                //.replace("{uuid}", _uuid)
                .replace("{vars}",String.format("%s=%s,", PLAYBACK_ID_NAME, playback_id)+"%s");
        final String file = prefix + path;

        _sendEvent.accept("FSPlayback", new PayloadFSPlayback(file));
        log.info("[{}]: rms: play [{}] as {}", _sessionId, file, playback_id);
        return playback_id;
    }

    @Override
    public void notifySentenceBegin(final PayloadSentenceBegin payload) {
        super.notifySentenceBegin(payload);
        /*
        _isUserSpeak.set(true);
        if (null != _sessionId) {
            log.info("[{}]: notifySentenceBegin: {}", _sessionId, payload);
        }
        if (_playback.get() != null && _lastReply != null && _lastReply.getPause_on_speak() != null && _lastReply.getPause_on_speak()) {
            _playback.get().pauseCurrent();
            log.info("[{}]: pauseCurrent: {}", _sessionId, _playback.get());
        }
         */
    }

    @Override
    public void notifyTranscriptionResultChanged(final PayloadTranscriptionResultChanged payload) {
        super.notifyTranscriptionResultChanged(payload);
    }

    @Override
    public void notifySentenceEnd(final PayloadSentenceEnd payload) {
        super.notifySentenceEnd(payload);
        /*
        _isUserSpeak.set(false);
        _idleStartInMs.set(System.currentTimeMillis());

        if (null != _sessionId) {
            log.info("[{}]: notifySentenceEnd: {}", _sessionId, payload);
        }

        if (_playback.get() != null && _lastReply != null && _lastReply.getPause_on_speak() != null && _lastReply.getPause_on_speak()) {
            _playback.get().resumeCurrent();
            log.info("[{}]: resumeCurrent: {}", _sessionId, _playback.get());
        }

        if (_sessionId != null) {
            boolean isAiSpeaking = _playback.get() != null && _playback.get().isPlaying();
            try {
                final ApiResponse<AIReplyVO> response =
                        _scriptApi.ai_reply(_sessionId, payload.getResult(), null, isAiSpeaking ? 1 : 0);
                if (response.getData() != null) {
                    if (doPlayback(response.getData())) {
                        _lastReply = response.getData();
                    }
                } else {
                    log.info("[{}]: notifySentenceEnd: ai_reply {}, do nothing\n", _sessionId, response);
                }
            } catch (Exception ex) {
                log.warn("[{}]: notifySentenceEnd: ai_reply error, detail: {}", _sessionId, ex.toString());
            }
        }
         */
    }

    private final BiConsumer<String, Object> _sendEvent;
    private final ScriptApi _scriptApi;
    private final String _welcome;
    private final String _rms_cp_prefix;
    private final String _rms_tts_prefix;
    private final String _rms_wav_prefix;

    private AIReplyVO _lastReply;

    private final AtomicReference<String> _currentPlaybackId = new AtomicReference<>(null);
    private final AtomicReference<String> _currentAIContentId = new AtomicReference<>(null);

    private ScheduledExecutorService _delayExecutor = null;
    private long _testDelayMs = 0;
    private long _testDisconnectTimeout = -1;
    private final Runnable _doDisconnect;
}
