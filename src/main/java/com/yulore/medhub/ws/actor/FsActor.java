package com.yulore.medhub.ws.actor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mgnt.utils.StringUnicodeEncoderDecoder;
import com.yulore.medhub.api.AIReplyVO;
import com.yulore.medhub.api.ApiResponse;
import com.yulore.medhub.api.ScriptApi;
import com.yulore.medhub.session.ASRActor;
import com.yulore.medhub.vo.*;
import com.yulore.medhub.ws.HandlerUrlBuilder;
import com.yulore.medhub.ws.WsHandler;
import com.yulore.util.ExceptionUtil;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

@ToString
@Slf4j
public abstract class FsActor extends ASRActor implements WsHandler {
    static final long CHECK_IDLE_TIMEOUT = 5000L; // 5 seconds to report check idle to script engine
    final static String PLAYBACK_ID_NAME="vars_playback_id";

    static public FsActor findBy(final String sessionId) {
        return _fsSessions.get(sessionId);
    }

    @Override
    public void onAttached(final WebSocket webSocket) {

    }

    @Override
    public void onClose(final WebSocket webSocket) {
        stopAndCloseTranscriber();
        close();
    }

    public FsActor(final String uuid,
                   final String sessionId,
                   final ScriptApi scriptApi,
                   final String welcome,
                   final String recordStartTimestamp,
                   final String rms_cp_prefix,
                   final String rms_tts_prefix,
                   final String rms_wav_prefix,
                   final HandlerUrlBuilder handlerUrlBuilder,
                   final boolean testEnableDelay,
                   final long testDelayMs,
                   final boolean testEnableDisconnect,
                   final float testDisconnectProbability,
                   final Runnable doDisconnect) {
        _uuid = uuid;
        _sessionId = sessionId;
        _sendEvent = null;
        _scriptApi = scriptApi;
        _welcome = welcome;
        if (recordStartTimestamp != null && !recordStartTimestamp.isEmpty()) {
            final long rst = Long.parseLong(recordStartTimestamp);
            if (rst > 0) {
                // Microseconds -> Milliseconds
                _recordStartInMs.set(rst / 1000L);
                log.info("[{}]: FsSession: recordStartTimestamp: {} ms", _sessionId, _recordStartInMs.get());
            }
        }
        _rms_cp_prefix = rms_cp_prefix;
        _rms_tts_prefix = rms_tts_prefix;
        _rms_wav_prefix = rms_wav_prefix;
        _handlerUrlBuilder = handlerUrlBuilder;
        if (testEnableDelay) {
            _delayExecutor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("delayExecutor"));
            _testDelayMs = testDelayMs;
        }
        if (testEnableDisconnect && Math.random() < testDisconnectProbability) {
            _testDisconnectTimeout = (long)(Math.random() * 5000) + 5000L; // 5s ~ 10s
            log.info("[{}]: enable disconnect test feature, timeout: {} ms", sessionId, _testDisconnectTimeout);
        }
        _doDisconnect = doDisconnect;
        _fsSessions.put(_sessionId, this);
    }

    public void attachPlaybackWs(final BiConsumer<String, Object> sendEvent) {
        _sendEvent = sendEvent;
    }

    @Override
    public boolean transmit(final ByteBuffer bytes) {
        if ( !_isTranscriptionStarted.get() || _isTranscriptionFailed.get()) {
            if (_recvFirstAudioDataInMs.compareAndSet(0, 1)) {
                _recvFirstAudioDataInMs.set(System.currentTimeMillis());
            }
            return false;
        }

        final Consumer<ByteBuffer> transmitter = _transmitData.get();

        if (transmitter != null) {
            try {
                if (_delayExecutor == null) {
                    if (_asrStartedInMs.compareAndSet(0, 1)) {
                        // ref: https://help.aliyun.com/zh/isi/developer-reference/websocket#sectiondiv-iry-g6n-uqt
                        _asrStartedInMs.set(System.currentTimeMillis());
                        log.info("[{}]: start_asr_from_recv_first_audio_data: {} ms", _sessionId, _asrStartedInMs.get() - _recvFirstAudioDataInMs.get());
                    }
                    transmitter.accept(bytes);
                } else {
                    _delayExecutor.schedule(() -> {
                        if (_asrStartedInMs.compareAndSet(0, 1)) {
                            // ref: https://help.aliyun.com/zh/isi/developer-reference/websocket#sectiondiv-iry-g6n-uqt
                            _asrStartedInMs.set(System.currentTimeMillis());
                            log.info("[{}]: start_asr_from_recv_first_audio_data: {} ms", _sessionId, _asrStartedInMs.get() - _recvFirstAudioDataInMs.get());
                        }
                        transmitter.accept(bytes);
                    }, _testDelayMs, TimeUnit.MILLISECONDS);
                }
                _transmitCount.incrementAndGet();
            } catch (Exception ex) {
                log.warn("[{}]: transmit error, detail: {}", _sessionId, ex.toString());
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void close() {
        if (_delayExecutor != null) {
            _delayExecutor.shutdownNow();
            _delayExecutor = null;
        }
        super.close();
    }

    public void checkIdle() {
        final long idleTime = System.currentTimeMillis() - _idleStartInMs.get();
        if (_sessionId != null      // user answered
            && !_isUserSpeak.get()  // user not speak
            && !isAiSpeaking()      // AI not speak
            // && _aiSetting != null
        ) {
            if (idleTime > CHECK_IDLE_TIMEOUT) {
                log.info("[{}]: checkIdle: idle duration: {} ms >=: [{}] ms", _sessionId, idleTime, CHECK_IDLE_TIMEOUT);
                try {
                    final ApiResponse<AIReplyVO> response =
                            _scriptApi.ai_reply(_sessionId, null, idleTime, 0, null, 0);
                    log.info("[{}]: checkIdle: ai_reply {}", _sessionId, response);
                    if (response.getData() != null) {
                        if (doPlayback(response.getData())) {
                            // _lastReply = response.getData();
                        }
                    } else {
                        log.info("[{}]: checkIdle: ai_reply's data is null, do_nothing", _sessionId);
                    }
                } catch (final Exception ex) {
                    log.warn("[{}]: checkIdle: ai_reply error, detail: {}", _sessionId, ExceptionUtil.exception2detail(ex));
                }
            }
        }
        log.info("[{}]: checkIdle: is_speaking: {}/is_playing: {}/idle duration: {} ms",
                _sessionId, _isUserSpeak.get(), isAiSpeaking(), idleTime);
    }

    @Override
    public boolean transcriptionStarted() {
        if (super.transcriptionStarted()) {
            try {
                final ApiResponse<AIReplyVO> response =
                        _scriptApi.ai_reply(_sessionId, _welcome, null, 0, null, 0);
                if (response.getData() != null) {
                    if (doPlayback(response.getData())) {
                        // _lastReply = response.getData();
                    }
                } else {
                    _sendEvent.accept("FSHangup", new PayloadFSHangup(_uuid, _sessionId));
                    log.warn("[{}]: transcriptionStarted: ai_reply({}), hangup", _sessionId, response);
                }
            } catch (final Exception ex) {
                _sendEvent.accept("FSHangup", new PayloadFSHangup(_uuid, _sessionId));
                log.warn("[{}]: transcriptionStarted: ai_reply error, hangup, detail: {}", _sessionId, ExceptionUtil.exception2detail(ex));
            }
            return true;
        } else {
            log.warn("[{}]: transcriptionStarted called already, ignore", _sessionId);
            return false;
        }
    }

    public void notifyFSPlaybackStarted(final WSCommandVO cmd) {
        final String playbackId = cmd.getPayload().get("playback_id");
        final long playbackStartedInMs = System.currentTimeMillis();
        memoFor(playbackId).setBeginInMs(playbackStartedInMs);

        final String currentPlaybackId = _currentPlaybackId.get();
        if (currentPlaybackId != null) {
            if (currentPlaybackId.equals(playbackId)) {
                _currentPlaybackDuration.set(()->System.currentTimeMillis() - playbackStartedInMs);
                log.info("[{}]: notifyFSPlaybackStarted => current playbackid: {} Matched",
                        _sessionId, playbackId);
            } else {
                log.info("[{}]: notifyFSPlaybackStarted => current playbackid: {} mismatch started playbackid: {}, ignore",
                        _sessionId, currentPlaybackId, playbackId);
            }
        } else {
            log.warn("[{}]: currentPlaybackId is null BUT notifyFSPlaybackStarted with: {}", _sessionId, playbackId);
        }
    }

    public void notifyPlaybackResumed(final WSCommandVO cmd) {
        final String playbackId = cmd.getPayload().get("playback_id");
        final String currentPlaybackId = _currentPlaybackId.get();
        if (currentPlaybackId != null) {
            if (currentPlaybackId.equals(playbackId)) {
                final String str_playback_duration  = cmd.getPayload().get("playback_duration");
                final long playbackDurationInMs = str_playback_duration != null ? Long.parseLong(str_playback_duration) : 0;
                final long playbackResumedInMs = System.currentTimeMillis();
                _currentPlaybackDuration.set(()->playbackDurationInMs + (System.currentTimeMillis() - playbackResumedInMs));
                log.info("[{}]: notifyPlaybackResumed => current playbackid: {} Matched / playback_duration: {} ms",
                        _sessionId, playbackId, playbackDurationInMs);
            } else {
                log.info("[{}]: notifyPlaybackResumed => current playbackid: {} mismatch resumed playbackid: {}, ignore",
                        _sessionId, currentPlaybackId, playbackId);
            }
        } else {
            log.warn("[{}]: currentPlaybackId is null BUT notifyPlaybackResumed with: {}", _sessionId, playbackId);
        }
    }

    public void notifyPlaybackPaused(final WSCommandVO cmd) {
        final String playbackId = cmd.getPayload().get("playback_id");
        final String currentPlaybackId = _currentPlaybackId.get();
        if (currentPlaybackId != null) {
            if (currentPlaybackId.equals(playbackId)) {
                final String str_playback_duration  = cmd.getPayload().get("playback_duration");
                final long playbackDurationInMs = str_playback_duration != null ? Long.parseLong(str_playback_duration) : 0;
                _currentPlaybackDuration.set(()->playbackDurationInMs);
                log.info("[{}]: notifyPlaybackPaused => current playbackid: {} Matched / playback_duration: {} ms",
                        _sessionId, playbackId, playbackDurationInMs);
            } else {
                log.info("[{}]: notifyPlaybackPaused => current playbackid: {} mismatch paused playbackid: {}, ignore",
                        _sessionId, currentPlaybackId, playbackId);
            }
        } else {
            log.warn("[{}]: currentPlaybackId is null BUT notifyPlaybackPaused with: {}", _sessionId, playbackId);
        }
    }

    public void notifyFSPlaybackStopped(final WSCommandVO cmd) {
        final String playbackId = cmd.getPayload().get("playback_id");
        if (_currentPlaybackId.get() != null
            && playbackId != null
            && playbackId.equals(_currentPlaybackId.get()) ) {
            _currentPlaybackId.set(null);
            _currentPlaybackDuration.set(()->0L);
            _idleStartInMs.set(System.currentTimeMillis());
            log.info("[{}]: notifyFSPlaybackStopped: current playback_id matched:{}, clear current PlaybackId", _sessionId, playbackId);
            if (memoFor(playbackId).isHangup()) {
                // hangup call
                _sendEvent.accept("FSHangup", new PayloadFSHangup(_uuid, _sessionId));
            }
        } else {
            log.info("!NOT! current playback_id:{}, ignored", playbackId);
        }
    }

    private boolean doPlayback(final AIReplyVO replyVO) {
        if (replyVO.getVoiceMode() == null || replyVO.getAi_content_id() == null) {
            return false;
        }

        final String newPlaybackId = UUID.randomUUID().toString();
        final String ai_content_id = Long.toString(replyVO.getAi_content_id());
        log.info("[{}]: doPlayback: {}", _sessionId, replyVO);

        final String file = aireply2file(replyVO,
                ()->String.format("%s=%s,content_id=%s,vars_start_timestamp=%d,playback_idx=%d",
                PLAYBACK_ID_NAME, newPlaybackId, ai_content_id, System.currentTimeMillis() * 1000L, 0),
                newPlaybackId);

        if (file != null) {
            final String prevPlaybackId = _currentPlaybackId.getAndSet(null);
            if (prevPlaybackId != null) {
                _sendEvent.accept("FSStopPlayback", new PayloadFSChangePlayback(_uuid, prevPlaybackId));
            }

            //_currentAIContentId.set(ai_content_id);
            _currentPlaybackId.set(newPlaybackId);
            _currentPlaybackPaused.set(false);
            _currentPlaybackDuration.set(()->0L);
            createPlaybackMemo(newPlaybackId,
                    replyVO.getAi_content_id(),
                    replyVO.getCancel_on_speak(),
                    replyVO.getHangup() == 1);
            _sendEvent.accept("FSStartPlayback", new PayloadFSStartPlayback(_uuid, newPlaybackId, ai_content_id, file));
            log.info("[{}]: fs_play [{}] as {}", _sessionId, file, newPlaybackId);
            return true;
        } else {
            return false;
        }
    }

    private String aireply2file(final AIReplyVO vo, final Supplier<String> vars, final String playback_id) {
        if ("cp".equals(vo.getVoiceMode())) {
            return _rms_cp_prefix.replace("{cpvars}", tryExtractCVOS(vo))
                    .replace("{uuid}", _uuid)
                    .replace("{rrms}", _handlerUrlBuilder.get("readRms"))
                    .replace("{vars}", vars.get())
                    + playback_id + ".wav"
                    ;
        }

        if ("tts".equals(vo.getVoiceMode())) {
            return _rms_tts_prefix
                    .replace("{uuid}", _uuid)
                    .replace("{rrms}", _handlerUrlBuilder.get("readRms"))
                    .replace("{vars}", String.format("text=%s,%s",
                            StringUnicodeEncoderDecoder.encodeStringToUnicodeSequence(vo.getReply_content()),
                            vars.get()))
                    + playback_id + ".wav"
                    ;
        }

        if ("wav".equals(vo.getVoiceMode())) {
            return _rms_wav_prefix
                    .replace("{uuid}", _uuid)
                    .replace("{rrms}", _handlerUrlBuilder.get("readRms"))
                    .replace("{vars}", vars.get())
                    + vo.getAi_speech_file();
        }

        return null;
    }

    private String tryExtractCVOS(final AIReplyVO vo) {
        try {
            return new ObjectMapper().writeValueAsString(vo.getCps());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void notifySentenceBegin(final PayloadSentenceBegin payload) {
        super.notifySentenceBegin(payload);
        _isUserSpeak.set(true);
        _currentSentenceBeginInMs.set(System.currentTimeMillis());
        if (isAICancelOnSpeak()) {
            final String playback_id = _currentPlaybackId.get();
            _sendEvent.accept("FSStopPlayback", new PayloadFSChangePlayback(_uuid, playback_id));
            log.info("[{}]: stop current playing ({}) for cancel_on_speak is true", _sessionId, playback_id);
        }
    }

    private boolean isAICancelOnSpeak() {
        final String playbackId = _currentPlaybackId.get();
        if (playbackId != null) {
            return memoFor(playbackId).isCancelOnSpeak();
        } else {
            return false;
        }
    }

    @Override
    public void notifyTranscriptionResultChanged(final PayloadTranscriptionResultChanged payload) {
        super.notifyTranscriptionResultChanged(payload);
        /*
        if (isAiSpeaking()) {
            final int length = payload.getResult().length();
            if (length >= 10) {
                if ( (length - countChinesePunctuations(payload.getResult())) >= 10  && _currentPlaybackPaused.compareAndSet(false, true)) {
                    _sendEvent.accept("FSPausePlayback", new PayloadFSChangePlayback(_uuid, _currentPlaybackId.get()));
                    log.info("[{}]: notifyTranscriptionResultChanged: pause_current for result {} text >= 10", _sessionId, payload.getResult());
                }
            }
        }*/
    }

    @Override
    public void notifySentenceEnd(final PayloadSentenceEnd payload) {
        super.notifySentenceEnd(payload);

        final long sentenceEndInMs = System.currentTimeMillis();
        _isUserSpeak.set(false);
        _idleStartInMs.set(sentenceEndInMs);

        String userContentId = null;
        try {
            final boolean isAiSpeaking = isAiSpeaking();
            final String userSpeechText = payload.getResult();
            final String aiContentId = currentAiContentId();
            final int speakingDuration = currentSpeakingDuration();

            log.info("[{}]: before ai_reply: speech:{}/is_speaking:{}/content_id:{}/speaking_duration:{} s",
                    _sessionId, userSpeechText, isAiSpeaking, aiContentId, (float)speakingDuration / 1000.0f);
            final ApiResponse<AIReplyVO> response =
                    _scriptApi.ai_reply(_sessionId, userSpeechText, null, isAiSpeaking ? 1 : 0, aiContentId, speakingDuration);
            log.info("[{}]: notifySentenceEnd: ai_reply ({})", _sessionId, response);
            if (response.getData() != null) {
                if (response.getData().getUser_content_id() != null) {
                    userContentId = response.getData().getUser_content_id().toString();
                }
                if (doPlayback(response.getData())) {
                    // _lastReply = response.getData();
                } else {
                    if (response.getData().getHangup() == 1) {
                        _sendEvent.accept("FSHangup", new PayloadFSHangup(_uuid, _sessionId));
                        log.info("[{}]: notifySentenceEnd: hangup ({}) for ai_reply ({})", _sessionId, _sessionId, response.getData());
                    }
                    if (isAiSpeaking && _currentPlaybackPaused.compareAndSet(true, false) ) {
                        _sendEvent.accept("FSResumePlayback", new PayloadFSChangePlayback(_uuid, _currentPlaybackId.get()));
                        log.info("[{}]: notifySentenceEnd: resume_current ({}) for ai_reply ({}) without_new_ai_playback",
                                _sessionId, _currentPlaybackId.get(), payload.getResult());
                    }
                }
            } else {
                log.info("[{}]: notifySentenceEnd: ai_reply's data is null', do_nothing", _sessionId);
            }
        } catch (final Exception ex) {
            log.warn("[{}]: notifySentenceEnd: ai_reply error, detail: {}", _sessionId, ExceptionUtil.exception2detail(ex));
        }

        {
            // report USER speak timing
            final long start_speak_timestamp = _asrStartedInMs.get() + payload.getBegin_time();
            final long stop_speak_timestamp = _asrStartedInMs.get() + payload.getTime();
            final long user_speak_duration = stop_speak_timestamp - start_speak_timestamp;

            final ApiResponse<Void> resp = _scriptApi.report_content(
                    _sessionId,
                    userContentId,
                    payload.getIndex(),
                    "USER",
                    _recordStartInMs.get(),
                    start_speak_timestamp,
                    stop_speak_timestamp,
                    user_speak_duration);
            log.info("[{}]: user report_content ({})'s response: {}", _sessionId, userContentId, resp);
        }
        {
            // report ASR event timing
            // sentence_begin_event_time in Milliseconds
            final long begin_event_time = _currentSentenceBeginInMs.get() - _asrStartedInMs.get();

            // sentence_end_event_time in Milliseconds
            final long end_event_time = sentenceEndInMs - _asrStartedInMs.get();

            final ApiResponse<Void> resp = _scriptApi.report_asrtime(
                    _sessionId,
                    userContentId,
                    payload.getIndex(),
                    begin_event_time,
                    end_event_time);
            log.info("[{}]: user report_asrtime ({})'s response: {}", _sessionId, userContentId, resp);
        }
    }

    private boolean isAiSpeaking() {
        return _currentPlaybackId.get() != null;
    }

    private String currentAiContentId() {
        return isAiSpeaking() ? memoFor(_currentPlaybackId.get()).contentId : null;
    }

    private int currentSpeakingDuration() {
        return isAiSpeaking() ?  _currentPlaybackDuration.get().get().intValue() : 0;
    }

    public void notifyFSRecordStarted(final WSCommandVO cmd) {
        final String recordStartTimestamp = cmd.getPayload().get("record_start_timestamp");
        if (recordStartTimestamp != null && !recordStartTimestamp.isEmpty()) {
            final long rst = Long.parseLong(recordStartTimestamp);
            if (rst > 0) {
                // Microseconds -> Milliseconds
                _recordStartInMs.set(rst / 1000L);
                log.info("[{}]: notifyFSRecordStarted: recordStartTimestamp: {} ms", _sessionId, _recordStartInMs.get());
            }
        }
    }

    private BiConsumer<String, Object> _sendEvent;

    private final String _uuid;
    private final ScriptApi _scriptApi;
    private final String _welcome;
    private final String _rms_cp_prefix;
    private final String _rms_tts_prefix;
    private final String _rms_wav_prefix;
    private final HandlerUrlBuilder _handlerUrlBuilder;

    // private AIReplyVO _lastReply;

    private final AtomicReference<String> _currentPlaybackId = new AtomicReference<>(null);
    private final AtomicBoolean _currentPlaybackPaused = new AtomicBoolean(false);
    private final AtomicReference<Supplier<Long>> _currentPlaybackDuration = new AtomicReference<>(()->0L);

    private final AtomicLong _idleStartInMs = new AtomicLong(System.currentTimeMillis());
    private final AtomicBoolean _isUserSpeak = new AtomicBoolean(false);
    private final AtomicLong _asrStartedInMs = new AtomicLong(0);
    private final AtomicLong _recordStartInMs = new AtomicLong(-1);
    private final AtomicLong _currentSentenceBeginInMs = new AtomicLong(-1);

    private final AtomicLong _recvFirstAudioDataInMs = new AtomicLong(0);

    private ScheduledExecutorService _delayExecutor = null;
    private long _testDelayMs = 0;
    private long _testDisconnectTimeout = -1;
    private final Runnable _doDisconnect;

    private static final ConcurrentMap<String, FsActor> _fsSessions = new ConcurrentHashMap<>();
}
