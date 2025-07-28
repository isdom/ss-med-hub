package com.yulore.medhub.ws.actor;

import com.alibaba.nls.client.protocol.SampleRateEnum;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriber;
import com.google.common.base.Strings;
import com.yulore.medhub.api.*;
import com.yulore.medhub.service.ASRConsumer;
import com.yulore.medhub.service.ASROperator;
import com.yulore.medhub.service.ASRService;
import com.yulore.medhub.vo.PayloadPCMEvent;
import com.yulore.medhub.vo.PayloadSentenceBegin;
import com.yulore.medhub.vo.PayloadSentenceEnd;
import com.yulore.medhub.vo.PayloadTranscriptionResultChanged;
import com.yulore.medhub.vo.cmd.*;
import com.yulore.util.ByteArrayListInputStream;
import com.yulore.util.ExceptionUtil;
import com.yulore.util.VarsUtil;
import com.yulore.util.WaveUtil;
import io.micrometer.core.instrument.Timer;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;

import static org.springframework.beans.factory.config.ConfigurableBeanFactory.SCOPE_PROTOTYPE;

/**
 * Advanced Phone Operator Actor
 */
@Slf4j
@ToString
@Component
@Scope(SCOPE_PROTOTYPE)
public final class ApoActor {
    private static final AtomicInteger currentCounter = new AtomicInteger(0);

    private final int _actorIdx;

    private final Consumer<ApoActor> _callStarted;
    private ScheduledFuture<?> _checkFuture;

    public int actorIdx() {
        return _actorIdx;
    }

    public record RecordContext(String sessionId, String bucketName, String objectName, InputStream content) {
    }

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private String _sessionId;

    public String sessionId() {
        return _sessionId;
    }

    public String clientIp() {
        return _clientIp;
    }

    public String uuid() {
        return _uuid;
    }

    @Autowired
    private ASRService _asrService;

    private final AtomicReference<ASROperator> asrRef = new AtomicReference<>(null);

    private final Consumer<Runnable> _runOn;

    public interface Reply2Playback extends BiFunction<String, AIReplyVO, Supplier<Runnable>> {}

    private Reply2Playback _reply2playback;

    public interface Context {
        String clientIp();
        String uuid();
        String tid();
        // long answerInMss();
        Consumer<Runnable> runOn(int idx);
        Consumer<ApoActor> doHangup();
        Consumer<RecordContext> saveRecord();
        Consumer<ApoActor> callStarted();
    }

    public ApoActor(final Context ctx) {
        _actorIdx = currentCounter.incrementAndGet();
        _sessionBeginInMs = System.currentTimeMillis();

        _clientIp = ctx.clientIp();
        _uuid = ctx.uuid();
        _tid = ctx.tid();
        _runOn = ctx.runOn(actorIdx());
        _doHangup = ctx.doHangup();
        _doSaveRecord = ctx.saveRecord();
        _callStarted = ctx.callStarted();
        if (Strings.isNullOrEmpty(_uuid) || Strings.isNullOrEmpty(_tid)) {
            log.warn("[{}]: ApoActor_ctor error for uuid:{}/tid:{}, abort apply_session.", _clientIp, _sessionId, _uuid);
            return;
        }
    }

    public void start(final ScheduledFuture<?> checkFuture) {
        log.info("[{}]: [{}]-[{}] ApoActor start with CallApi({})/ScriptApi({})", _clientIp, _sessionId, _uuid, _callApi, _scriptApi);
        _checkFuture = checkFuture;
        try {
            final ApiResponse<ApplySessionVO> response = _callApi.apply_session(CallApi.ApplySessionRequest.builder()
                    .uuid(_uuid)
                    .tid(_tid)
                    .build());
            _sessionId = response.getData().getSessionId();
            log.info("[{}]: [{}]-[{}]: apply_session => response: {}", _clientIp, _sessionId, _uuid, response);
            _callStarted.accept(this);
            startTranscription();
        } catch (Exception ex) {
            log.warn("[{}]: [{}]-[{}]: failed for callApi.apply_session, detail: {}", _clientIp, _sessionId, _uuid,
                    ExceptionUtil.exception2detail(ex));
        }
    }

    public void notifyMockAnswer() {
        _runOn.accept(()-> {
            if (!isClosed.get()) {
                if (_isUserAnswered.get()) {
                    log.info("[{}]: [{}]-[{}]: notifyUserAnswer has called already, ignore notifyMockAnswer!", _clientIp, _sessionId, _uuid);
                    return;
                }
                if (Strings.isNullOrEmpty(_uuid) || Strings.isNullOrEmpty(_tid)) {
                    log.warn("[{}]/[{}]: doMockAnswer error for uuid:{}/tid:{}, abort mock_answer.", _clientIp, _sessionId, _uuid, _tid);
                    return;
                }
                try {
                    final ApiResponse<UserAnswerVO> response = _callApi.mock_answer(CallApi.MockAnswerRequest.builder()
                            .sessionId(_sessionId)
                            .uuid(_uuid)
                            .tid(_tid)
                            .answerTime(System.currentTimeMillis())
                            .build());
                    log.info("[{}]: [{}]-[{}]: mockAnswer: tid:{} => response: {}", _clientIp, _sessionId, _uuid, _tid, response);

                    saveAndPlayWelcome(response);
                } catch (Exception ex) {
                    log.warn("[{}]: [{}]-[{}]: failed for callApi.mock_answer, detail: {}", _clientIp, _sessionId, _uuid, ex.toString());
                }
            }
        });
    }

    public void notifyUserAnswer(final VOUserAnswer vo) {
        if (!_isUserAnswered.compareAndSet(false, true)) {
            log.warn("[{}]: [{}]-[{}]: notifyUserAnswer called already, ignore!", _clientIp, _sessionId, _uuid);
            return;
        }
        if (Strings.isNullOrEmpty(_uuid) || Strings.isNullOrEmpty(_tid)) {
            log.warn("[{}]/[{}]: ApoActor_notifyUserAnswer error for uuid:{}/tid:{}, abort user_answer.", _clientIp, _sessionId, _uuid, _tid);
            return;
        }
        try {
            final ApiResponse<UserAnswerVO> response = _callApi.user_answer(CallApi.UserAnswerRequest.builder()
                            .sessionId(_sessionId)
                            .kid(vo.kid)
                            .tid(vo.tid)
                            .realName(vo.realName)
                            .genderStr(vo.genderStr)
                            .aesMobile(vo.aesMobile)
                            .answerTime(System.currentTimeMillis())
                            .build());
            log.info("[{}]: [{}]-[{}]: userAnswer: kid:{}/tid:{}/realName:{}/gender:{}/aesMobile:{} => response: {}",
                    _clientIp, _sessionId, _uuid, vo.kid, vo.tid, vo.realName, vo.genderStr, vo.aesMobile, response);

            saveAndPlayWelcome(response);
        } catch (Exception ex) {
            log.warn("[{}]: [{}]-[{}]: failed for callApi.user_answer, detail: {}", _clientIp, _sessionId, _uuid, ex.toString());
        }
    }

    private void saveAndPlayWelcome(final ApiResponse<UserAnswerVO> response) {
        if (null != response.getData()) {
            _welcome.set(response.getData());
            if (null != response.getData()) {
                _aiSetting = response.getData().getAiSetting();
            }
            tryPlayWelcome();
        }
    }

    public void attachPlaybackWs(final Reply2Playback reply2playback, final BiConsumer<String, Object> sendEvent) {
        _reply2playback = reply2playback;
        _sendEvent = sendEvent;
        tryPlayWelcome();
    }

    private void tryPlayWelcome() {
        if (_reply2playback != null && _welcome.get() != null && asrRef.get() != null) {
            final AIReplyVO welcome = _welcome.getAndSet(null);
            if (null != welcome) {
                doPlayback(welcome);
            }
        }
    }

    private void startTranscription() {
        _asrService.startTranscription(new ASRConsumer() {
            @Override
            public void onSpeechTranscriberCreated(SpeechTranscriber speechTranscriber) {
                speechTranscriber.setSampleRate(SampleRateEnum.SAMPLE_RATE_16K);
                speechTranscriber.addCustomedParam("speech_noise_threshold", 0.9);
            }

            @Override
            public void onSentenceBegin(PayloadSentenceBegin payload) {
                _runOn.accept(()-> {
                    log.info("apo_io => onSentenceBegin: {}", payload);
                    whenASRSentenceBegin(payload);
                });
            }

            @Override
            public void onTranscriptionResultChanged(PayloadTranscriptionResultChanged payload) {
                log.info("apo_io => onTranscriptionResultChanged: {}", payload);
            }

            @Override
            public void onSentenceEnd(final PayloadSentenceEnd payload) {
                final long sentenceEndInMs = System.currentTimeMillis();
                _runOn.accept(()->{
                    log.info("apo_io => onSentenceEnd: {}", payload);
                    whenASRSentenceEnd(payload, sentenceEndInMs);
                });
            }

            @Override
            public void onTranscriberFail(Object reason) {
                log.warn("[{}] apo_io => onTranscriberFail: {}", _sessionId,
                        reason != null ? reason.toString() : "(null)");
            }
        }).whenComplete((asrOperator, ex) -> _runOn.accept(()->{
            if (ex != null) {
                log.warn("startTranscription failed", ex);
            } else {
                asrRef.set(asrOperator);
                if (isClosed.get()) {
                    // means close() method has been called
                    final var asr = asrRef.getAndSet(null);
                    if (asr != null) { // means when close() called, operator !NOT! set yet
                        asr.close();
                    }
                } else {
                    // apo session not closed
                    tryPlayWelcome();
                }
            }
        }));
    }

    private boolean isAiSpeaking() {
        return _currentPlaybackId.get() != null;
    }

    public void checkIdle() {
        _runOn.accept(()-> {
            final long idleTime = System.currentTimeMillis() - _idleStartInMs.get();
            boolean isAiSpeaking = isAiSpeaking();
            if (_isUserAnswered.get()  // user answered
                && _reply2playback != null  // playback ws has connected
                && !_isUserSpeak.get()  // user not speak
                && !isAiSpeaking        // AI not speak
                && _aiSetting != null
                ) {
                if (idleTime > _aiSetting.getIdle_timeout()) {
                    log.info("[{}]: [{}]-[{}]: checkIdle: idle duration: {} ms >=: [{}] ms", _clientIp, _sessionId, _uuid, idleTime, _aiSetting.getIdle_timeout());
                    try {
                        final ApiResponse<AIReplyVO> response =
                                _scriptApi.ai_reply(_sessionId, null, null, idleTime, 0, null, 0);
                        if (response.getData() != null) {
                            if (response.getData().getAi_content_id() != null && doPlayback(response.getData())) {
                                return;
                            } else {
                                log.info("[{}]: [{}]-[{}]: checkIdle: ai_reply {}, !NOT! doPlayback", _clientIp, _sessionId, _uuid, response);
                                if (response.getData().getHangup() == 1) {
                                    // hangup call
                                    _doHangup.accept(this);
                                }
                            }
                        } else {
                            log.info("[{}]: [{}]-[{}]: checkIdle: ai_reply {}, do nothing", _clientIp, _sessionId, _uuid, response);
                        }
                    } catch (Exception ex) {
                        log.warn("[{}]: [{}]-[{}]: checkIdle: ai_reply error, detail: {}", _clientIp, _sessionId, _uuid,
                                ExceptionUtil.exception2detail(ex));
                    }
                }
            }
            log.info("[{}]: [{}]-[{}]: checkIdle: is_speaking: {}/is_playing: {}/idle duration: {} ms",
                    _clientIp, _sessionId, _uuid, _isUserSpeak.get(), isAiSpeaking, idleTime);
        /*
        if (!_isUserSpeak.get() && isAiSpeaking() && _currentPlaybackPaused.get() && idleTime >= 2000) {
            // user not speaking & ai speaking and paused and user not speak more than 2s
            if (_currentPlaybackPaused.compareAndSet(true, false)) {
                _sendEvent.accept("PCMResumePlayback", new PayloadPCMEvent(_currentPlaybackId.get(), ""));
                log.info("[{}]: checkIdle: resume current {}", _sessionId, _currentPlaybackId.get());
            }
        }
        */
        });
    }

    public void transmit(final ByteBuffer buf) {
        if (!isClosed.get()) {
            final var operator = asrRef.get();
            if (operator != null) {
                try {
                    final byte[] frame = new byte[buf.remaining()];
                    buf.get(frame);
                    operator.transmit(frame);
                    if (_asrStartInMs.compareAndSet(0, 1)) {
                        _asrStartInMs.set(System.currentTimeMillis());
                    }
                    _usBufs.add(frame);
                } catch (Exception ex) {
                    log.warn("[{}]: [{}]-[{}]: transmit_asr_failed, detail: {}", _clientIp, _sessionId, _uuid,
                            ExceptionUtil.exception2detail(ex));
                }
                /* TODO: comment for now
                final long now = System.currentTimeMillis();
                transmitDelayPer50 += now - fsReadFrameInMss / 1000L;
                handleCostPer50 += now - recvdInMs;
                transmitCount++;
                if ( transmitCount >= 50 ) {
                    // record delay & cost each 50 times
                    td_timer.record(transmitDelayPer50, TimeUnit.MILLISECONDS);
                    hc_timer.record(handleCostPer50, TimeUnit.MILLISECONDS);
                    // reset counter / delay / cost
                    transmitCount = 0;
                    transmitDelayPer50 = 0;
                    handleCostPer50 = 0;
                }
                if (_asrStartedInMs.compareAndSet(0, now)) {
                    // ref: https://help.aliyun.com/zh/isi/developer-reference/websocket#sectiondiv-iry-g6n-uqt
                    log.info("[{}]: asr audio start at {} ms", sessionId, _asrStartedInMs.get());
                }
                 */
            }
        }
    }

    private String currentAiContentId() {
        return isAiSpeaking() ? memoFor(_currentPlaybackId.get()).contentId : null;
    }

    private int currentSpeakingDuration() {
        return isAiSpeaking() ?  _currentPlaybackDuration.get().get().intValue() : 0;
    }

    public void notifyPlaybackSendStart(final String playbackId, final long startTimestamp) {
        memoFor(playbackId).setBeginInMs(startTimestamp);
        _currentPlaybackDuration.set(()->System.currentTimeMillis() - startTimestamp);
        if (!_currentPS.compareAndSet(null, new PlaybackSegment(startTimestamp))) {
            log.warn("[{}]: [{}]-[{}]: notifyPlaybackSendStart: current PlaybackSegment is !NOT! null", _clientIp, _sessionId, _uuid);
        } else {
            log.info("[{}]: [{}]-[{}]: notifyPlaybackSendStart: add new PlaybackSegment", _clientIp, _sessionId, _uuid);
        }
    }

    public void notifyPlaybackSendStop(final String playbackId, final long stopTimestamp) {
        final PlaybackSegment ps = _currentPS.getAndSet(null);
        if (ps != null) {
            _dsBufs.add(ps);
            log.info("[{}]: [{}]-[{}]: notifyPlaybackSendStop: move current PlaybackSegment to _dsBufs", _clientIp, _sessionId, _uuid);
        } else {
            log.warn("[{}]: [{}]-[{}]: notifyPlaybackSendStop: current PlaybackSegment is null", _clientIp, _sessionId, _uuid);
        }
    }

    public void notifyPlaybackSendData(final byte[] bytes) {
        final PlaybackSegment ps = _currentPS.get();
        if (ps != null) {
            ps._data.add(bytes);
        } else {
            log.warn("[{}]: [{}]-[{}]: notifyPlaybackSendData: current PlaybackSegment is null", _clientIp, _sessionId, _uuid);
        }
    }

    /*
    @Override
    public void notifySentenceBegin(final PayloadSentenceBegin payload) {
        super.notifySentenceBegin(payload);
        log.info("[{}]: [{}]-[{}]: notifySentenceBegin: {}", _clientIp, _sessionId, _uuid, payload);
        _isUserSpeak.set(true);
        _currentSentenceBeginInMs.set(System.currentTimeMillis());
        if (isAICancelOnSpeak()) {
            final Runnable stopPlayback = _currentStopPlayback.getAndSet(null);
            if (stopPlayback != null) {
                stopPlayback.run();
            }
            _sendEvent.accept("PCMStopPlayback", new PayloadPCMEvent(_currentPlaybackId.get(), ""));
            log.info("[{}]: [{}]-[{}]: stop current playing ({}) for cancel_on_speak is true", _clientIp, _sessionId, _uuid, _currentPlaybackId.get());
        }
    }
    */

    private void whenASRSentenceBegin(final PayloadSentenceBegin payload) {
        log.info("[{}]: [{}]-[{}]: notifySentenceBegin: {}", _clientIp, _sessionId, _uuid, payload);
        _isUserSpeak.set(true);
        _currentSentenceBeginInMs.set(System.currentTimeMillis());
        if (isAICancelOnSpeak()) {
            final Runnable stopPlayback = _currentStopPlayback.getAndSet(null);
            if (stopPlayback != null) {
                stopPlayback.run();
            }
            _sendEvent.accept("PCMStopPlayback", new PayloadPCMEvent(_currentPlaybackId.get(), ""));
            log.info("[{}]: [{}]-[{}]: stop current playing ({}) for cancel_on_speak is true", _clientIp, _sessionId, _uuid, _currentPlaybackId.get());
        }

        /*
        _isUserSpeak.set(true);
        _currentSentenceBeginInMs.set(System.currentTimeMillis());
        if (isAICancelOnSpeak()) {
            final String playback_id = _currentPlaybackId.get();
            sendEvent.accept("StopPlayback",
                    AFSStopPlaybackEvent.builder()
                            .localIdx(localIdx)
                            .playback_id(playback_id)
                            .stopEventInMs(_currentSentenceBeginInMs.get())
                            .build()
            );
            log.info("[{}] whenASRSentenceBegin: stop current playing ({}) for cancel_on_speak is true", sessionId, playback_id);
        }
        */
    }

    private boolean isAICancelOnSpeak() {
        final String playbackId = _currentPlaybackId.get();
        if (playbackId != null) {
            return memoFor(playbackId).isCancelOnSpeak();
        } else {
            return false;
        }
    }

    /*
    @Override
    public void notifyTranscriptionResultChanged(final PayloadTranscriptionResultChanged payload) {
        super.notifyTranscriptionResultChanged(payload);
        if (isAiSpeaking()) {
            final int length = payload.getResult().length();
            if (length >= 10) {
                if ( (length - countChinesePunctuations(payload.getResult())) >= 10  && _currentPlaybackPaused.compareAndSet(false, true)) {
                    _sendEvent.accept("PCMPausePlayback", new PayloadPCMEvent(_currentPlaybackId.get(), ""));
                    log.info("[{}]: [{}]-[{}]: notifyTranscriptionResultChanged: pause_current ({}) for result {} text >= 10",
                            _clientIp, _sessionId, _uuid, _currentPlaybackId.get(), payload.getResult());
                }
            }
        }
    }
    */

    private void whenASRSentenceEnd(final PayloadSentenceEnd payload, final long sentenceEndInMs) {
        log.info("[{}]: [{}]-[{}]: whenASRSentenceEnd: {}", _clientIp, _sessionId, _uuid, payload);

        // final long sentenceEndInMs = System.currentTimeMillis();
        _isUserSpeak.set(false);
        _idleStartInMs.set(sentenceEndInMs);
        if (_sessionId != null && _reply2playback != null && _aiSetting != null) {
            // playback ws has connected && _aiSetting is valid
            if (payload.getResult() == null || payload.getResult().isEmpty()) {
                _emptyUserSpeechCount++;
                log.warn("[{}] whenASRSentenceEnd: skip ai_reply => [{}] speech_is_empty, total empty count: {}",
                        _sessionId, payload.getIndex(), _emptyUserSpeechCount);
                return;
            }

            final String userContentId = interactWithScriptEngine(payload);
            reportUserContent(payload, userContentId);
            reportAsrTime(payload, sentenceEndInMs, userContentId);
        } else {
            log.warn("[{}]: [{}]-[{}]: whenASRSentenceEnd but sessionId is null or playback not ready or aiSetting not ready => (reply2playback: {}), (aiSetting: {})",
                    _clientIp, _sessionId, _uuid, _reply2playback, _aiSetting);
        }

        if (_eslApi != null) {
            final var startInMs = System.currentTimeMillis();
            final var resp = _eslApi.search_ref(_esl_headers, payload.getResult(), 0.5f);
            log.info("[{}]: [{}]-[{}]: {} => ESL Response: {}, cost {} ms", _clientIp, _sessionId, _uuid, payload.getResult(), resp, System.currentTimeMillis() - startInMs);
        }
    }

    /*
    @Override
    public void notifySentenceEnd(final PayloadSentenceEnd payload) {
        super.notifySentenceEnd(payload);
        log.info("[{}]: [{}]-[{}]: notifySentenceEnd: {}", _clientIp, _sessionId, _uuid, payload);

        final long sentenceEndInMs = System.currentTimeMillis();
        _isUserSpeak.set(false);
        _idleStartInMs.set(sentenceEndInMs);
        if (_sessionId != null && _playbackOn != null && _aiSetting != null) {
            // playback ws has connected && _aiSetting is valid
            final String userContentId = interactWithScriptEngine(payload);
            reportUserContent(payload, userContentId);
            reportAsrTime(payload, sentenceEndInMs, userContentId);
        } else {
            log.warn("[{}]: [{}]-[{}]: notifySentenceEnd but sessionId is null or playback not ready or aiSetting not ready => _playbackOn: {}/aiSetting: {}",
                    _clientIp, _sessionId, _uuid, _playbackOn, _aiSetting);
        }
    }
    */

    private String interactWithScriptEngine(final PayloadSentenceEnd payload) {
        final boolean isAiSpeaking = isAiSpeaking();
        String userContentId = null;
        try {
            final String userSpeechText = payload.getResult();
            final String aiContentId = currentAiContentId();
            final int speakingDuration = currentSpeakingDuration();
            log.info("[{}]: [{}]-[{}]: ai_reply: speech:{}/is_speaking:{}/content_id:{}/speaking_duration:{} s",
                    _clientIp, _sessionId, _uuid, userSpeechText, isAiSpeaking, aiContentId, (float)speakingDuration / 1000.0f);
            final ApiResponse<AIReplyVO> response =
                    _scriptApi.ai_reply(_sessionId, payload.getIndex(), userSpeechText,null, isAiSpeaking ? 1 : 0, aiContentId, speakingDuration);

            if (response.getData() != null) {
                if (response.getData().getUser_content_id() != null) {
                    userContentId = response.getData().getUser_content_id().toString();
                }
                if (response.getData().getAi_content_id() != null && doPlayback(response.getData())) {
                    // _lastReply = response.getData();
                } else {
                    log.info("[{}]: [{}]-[{}]: interactWithScriptEngine: ai_reply {}, !NOT! doPlayback", _clientIp, _sessionId, _uuid, response);
                    if (response.getData().getHangup() == 1) {
                        // hangup call
                        _doHangup.accept(this);
                    } else if (isAiSpeaking && _currentPlaybackPaused.compareAndSet(true, false) ) {
                        _sendEvent.accept("PCMResumePlayback", new PayloadPCMEvent(_currentPlaybackId.get(), ""));
                        log.info("[{}]: [{}]-[{}]: notifySentenceEnd: resume_current ({}) for ai_reply ({}) without_new_ai_playback",
                                _clientIp, _sessionId, _uuid, _currentPlaybackId.get(), payload.getResult());
                    }
                }
            } else {
                log.info("[{}]: [{}]-[{}]: notifySentenceEnd: ai_reply {}, do_nothing", _clientIp, _sessionId, _uuid, response);
            }
        } catch (final Exception ex) {
            log.warn("[{}]: [{}]-[{}]: notifySentenceEnd: ai_reply error, detail: {}", _clientIp, _sessionId, _uuid, ExceptionUtil.exception2detail(ex));
        }
        return userContentId;
    }

    public void notifyPlaybackStart(final String playbackId) {
        log.info("[{}]: [{}]-[{}]: notifyPlaybackStart => playbackId: {} while currentPlaybackId: {}",
                _clientIp, _sessionId, _uuid, playbackId, _currentPlaybackId.get());
    }

    public void notifyPlaybackStarted(final VOPCMPlaybackStarted vo, final Timer playback_timer) {
        final PlaybackMemo playbackMemo = memoFor(vo.playback_id);
        playbackMemo.sampleWhenCreate.stop(playback_timer);

        final long playbackStartedInMs = System.currentTimeMillis();
        playbackMemo.setBeginInMs(playbackStartedInMs);

        log.info("[{}]: [{}]-[{}]: notifyPlaybackStarted => playbackId: {} while currentPlaybackId: {}",
                _clientIp, _sessionId, _uuid, vo.playback_id, _currentPlaybackId.get());
        final String currentPlaybackId = _currentPlaybackId.get();
        if (currentPlaybackId != null) {
            if (currentPlaybackId.equals(vo.playback_id)) {
                _currentPlaybackDuration.set(()->System.currentTimeMillis() - playbackStartedInMs);
                log.info("[{}]: [{}]-[{}]: notifyPlaybackStarted => current playbackid: {} Matched",
                        _clientIp, _sessionId, _uuid, vo.playback_id);
            } else {
                log.info("[{}]: [{}]-[{}]: notifyPlaybackStarted => current playbackid: {} mismatch started playbackid: {}, ignore",
                        _clientIp, _sessionId, _uuid, currentPlaybackId, vo.playback_id);
            }
        } else {
            log.warn("[{}]-[{}]: currentPlaybackId is null BUT notifyPlaybackStarted with: {}", _sessionId, _uuid, vo.playback_id);
        }
    }

    public void notifyPlaybackStop(final VOPCMPlaybackStopped vo) {
        final String currentPlaybackId = _currentPlaybackId.get();
        if (currentPlaybackId != null) {
            if (currentPlaybackId.equals(vo.playback_id)) {
                // reset playback status
                _currentPlaybackId.set(null);
                _currentStopPlayback.set(null);
                _currentPlaybackPaused.set(false);
                _currentPlaybackDuration.set(()->0L);
                _idleStartInMs.set(System.currentTimeMillis());
                log.info("[{}]: [{}]-[{}]: notifyPlaybackStop => current playbackid: {} Matched / memo: {}",
                        _clientIp, _sessionId, _uuid, vo.playback_id, memoFor(vo.playback_id));
                if (memoFor(vo.playback_id).isHangup()) {
                    // hangup call
                    _doHangup.accept(this);
                }
            } else {
                log.info("[{}]: [{}]-[{}]: notifyPlaybackStop => current playbackid: {} mismatch stopped playbackid: {}, ignore",
                        _clientIp, _sessionId, _uuid, currentPlaybackId, vo.playback_id);
            }
        } else {
            log.warn("[{}]: [{}]-[{}]: currentPlaybackId is null BUT notifyPlaybackStop with: {}", _clientIp, _sessionId, _uuid, vo.playback_id);
        }
        reportAIContent(vo.playback_id, vo.content_id, vo.playback_begin_timestamp, vo.playback_end_timestamp, vo.playback_duration);
    }

    private void reportUserContent(final PayloadSentenceEnd payload, final String userContentId) {
        // report USER speak timing
        // ASR-Sentence-Begin-Time in Milliseconds
        final long start_speak_timestamp = _asrStartInMs.get() + payload.getBegin_time();
        // ASR-Sentence-End-Time in Milliseconds
        final long stop_speak_timestamp = _asrStartInMs.get() + payload.getTime();
        final long user_speak_duration = stop_speak_timestamp - start_speak_timestamp;

        final var content_index = payload.getIndex() - _emptyUserSpeechCount;

        final ApiResponse<Void> resp = _scriptApi.report_content(
                _sessionId,
                userContentId,
                content_index,
                "USER",
                _asrStartInMs.get(),
                start_speak_timestamp,
                stop_speak_timestamp,
                user_speak_duration);
        log.info("[{}]: [{}]-[{}]: user report_content(content_id:{}/idx:{}/asr_start:{}/start_speak:{}/stop_speak:{}/speak_duration:{} s)'s resp: {}",
                _clientIp,
                _sessionId,
                _uuid,
                userContentId,
                content_index,
                _asrStartInMs.get(),
                start_speak_timestamp,
                stop_speak_timestamp,
                user_speak_duration / 1000.0,
                resp);
    }

    private void reportAsrTime(final PayloadSentenceEnd payload, final long sentenceEndInMs, final String userContentId) {
        // report ASR event timing
        // sentence_begin_event_time in Milliseconds
        final long begin_event_time = _currentSentenceBeginInMs.get() - _asrStartInMs.get();

        // sentence_end_event_time in Milliseconds
        final long end_event_time = sentenceEndInMs - _asrStartInMs.get();

        final var content_index = payload.getIndex() - _emptyUserSpeechCount;

        final ApiResponse<Void> resp = _scriptApi.report_asrtime(
                _sessionId,
                userContentId,
                content_index,
                begin_event_time,
                end_event_time);
        log.info("[{}]: user report_asrtime({})'s resp: {}", _sessionId, userContentId, resp);
    }

    private void reportAIContent(final String playbackId,
                                 final String contentId,
                                 final String playback_begin_timestamp,
                                 final String playback_end_timestamp,
                                 final String playback_duration) {
        // report AI speak timing
        final PlaybackMemo memo = memoFor(playbackId);
        if (memo == null) {
            log.warn("[{}]: [{}]-[{}]: notifyPlaybackStop: can't find PlaybackMemo by {}", _clientIp, _sessionId, _uuid, playbackId);
            return;
        }
        final long start_speak_timestamp = playback_begin_timestamp != null ? Long.parseLong(playback_begin_timestamp) : memo.beginInMs;
        final long stop_speak_timestamp = playback_end_timestamp != null ? Long.parseLong(playback_end_timestamp) : System.currentTimeMillis();
        final long ai_speak_duration = playback_duration != null
                ? (long)(Float.parseFloat(playback_duration) * 1000L)
                : (stop_speak_timestamp - start_speak_timestamp);

        final ApiResponse<Void> resp = _scriptApi.report_content(
                _sessionId,
                contentId,
                memo.playbackIdx,
                "AI",
                _asrStartInMs.get(),
                start_speak_timestamp,
                stop_speak_timestamp,
                ai_speak_duration);
        log.info("[{}]: [{}]-[{}]: ai report_content(content_id:{}/idx:{}/asr_start:{}/start_speak:{}/stop_speak:{}/speak_duration:{} s)'s resp: {}",
                _clientIp,
                _sessionId,
                _uuid,
                contentId,
                memo.playbackIdx,
                _asrStartInMs.get(),
                start_speak_timestamp,
                stop_speak_timestamp,
                ai_speak_duration / 1000.0,
                resp);
    }

    public void notifyPlaybackPaused(final VOPCMPlaybackPaused vo) {
        final String currentPlaybackId = _currentPlaybackId.get();
        if (currentPlaybackId != null) {
            if (currentPlaybackId.equals(vo.playback_id)) {
                final long ai_speak_duration = vo.playback_duration != null
                        ? (long)(Float.parseFloat(vo.playback_duration) * 1000L)
                        : 0;
                _currentPlaybackDuration.set(()->ai_speak_duration);
                log.info("[{}]: [{}]-[{}]: notifyPlaybackPaused => current playbackid: {} Matched / playback_duration: {} ms",
                        _clientIp, _sessionId, _uuid, vo.playback_id, ai_speak_duration);
            } else {
                log.info("[{}]: [{}]-[{}]: notifyPlaybackPaused => current playbackid: {} mismatch paused playbackid: {}, ignore",
                        _clientIp, _sessionId, _uuid, currentPlaybackId, vo.playback_id);
            }
        } else {
            log.warn("[{}]: [{}]-[{}]: currentPlaybackId is null BUT notifyPlaybackPaused with: {}",
                    _clientIp, _sessionId, _uuid, vo.playback_id);
        }
    }

    public void notifyPlaybackResumed(final VOPCMPlaybackResumed vo) {
        final String currentPlaybackId = _currentPlaybackId.get();
        if (currentPlaybackId != null) {
            if (currentPlaybackId.equals(vo.playback_id)) {
                final long ai_speak_duration = vo.playback_duration != null
                        ? (long)(Float.parseFloat(vo.playback_duration) * 1000L)
                        : 0;
                final long playbackResumedInMs = System.currentTimeMillis();
                _currentPlaybackDuration.set(()->ai_speak_duration + (System.currentTimeMillis() - playbackResumedInMs));
                log.info("[{}]: [{}]-[{}]: notifyPlaybackResumed => current playbackid: {} Matched / playback_duration: {} ms",
                        _clientIp, _sessionId, _uuid, vo.playback_id, ai_speak_duration);
            } else {
                log.info("[{}]: [{}]-[{}]: notifyPlaybackResumed => current playbackid: {} mismatch resumed playbackid: {}, ignore",
                        _clientIp, _sessionId, _uuid, currentPlaybackId, vo.playback_id);
            }
        } else {
            log.warn("[{}]: [{}]-[{}]: currentPlaybackId is null BUT notifyPlaybackResumed with: {}",
                    _clientIp, _sessionId, _uuid, vo.playback_id);
        }
    }

    public void close() {
        try {
            if (isClosed.compareAndSet(false, true)) {
                if (null != _checkFuture) {
                    _checkFuture.cancel(false);
                }

                final ASROperator asr = asrRef.getAndSet(null);
                if (null != asr) {
                    asr.close();
                }

                /* TODO: restore close action
                if (isAiSpeaking()) {
                    _pendingReports.add(buildAIReport(_currentPlaybackId.get(), currentSpeakingDuration()));
                }

                if (removeLocalVO != null && _recordStartedVO != null) {
                    final long event_rst = _recordStartedVO.recordStartInMss / 1000L;
                    final long rms_rst = _recordStartedVO.fbwInMss / 1000L - (_recordStartedVO.fbwBytes / 640 * 20);
                    final long rsp_rst_diff = removeLocalVO.recordStopInMss / 1000L - rms_rst;
                    final long record_duration = removeLocalVO.recordDurationInMs;
                    final float scale = (float) record_duration / (float) rsp_rst_diff;

                    log.info("[{}] batch report_content: (rms_rst-event_rst): {} ms, (rsp - rms_rst): {} ms, record duration: {} ms, scale_factor: {}",
                            sessionId, rms_rst - event_rst, rsp_rst_diff, record_duration, scale);

                    final var ctx = new AfsActor.ReportContext() {
                        public long event_rst() {
                            return event_rst;
                        }

                        public long rms_rst() {
                            return rms_rst;
                        }

                        public float scale_factor() {
                            return scale;
                        }
                    };
                    for (final var doReport : _pendingReports) {
                        doReport.accept(ctx);
                    }
                } else {
                    log.warn("[{}] AfsActor.close(...) without removeLocalVO({}) or _recordStartedVO({}), skip batch report_content",
                            sessionId, removeLocalVO, _recordStartedVO);
                }
                */
                _id2memo.clear();
                //_pendingReports.clear();
                //_recordStartedVO = null;

                if (_sessionId != null) {
                    final String currentPlaybackId = _currentPlaybackId.getAndSet(null);
                    if (null != currentPlaybackId) {
                        // stop current playback when call close()
                        final Runnable stopPlayback = _currentStopPlayback.getAndSet(null);
                        if (stopPlayback != null) {
                            stopPlayback.run();
                        }
                        _sendEvent.accept("PCMStopPlayback", new PayloadPCMEvent(currentPlaybackId, ""));
                    }
                    generateRecordAndUpload();
                } else {
                    log.warn("[{}]: [{}]: close_without_valid_sessionId", _clientIp, _uuid);
                }
                if (!_isUserAnswered.get()) {
                    log.warn("[{}]: [{}]-[{}]: close_without_user_answer, total duration {} s", _clientIp, _sessionId, _uuid, (System.currentTimeMillis() - _sessionBeginInMs) / 1000.0f);
                }
                if (_reply2playback == null) {
                    log.warn("[{}]: [{}]-[{}]: close_without_playback_ws, total duration {} s", _clientIp, _sessionId, _uuid, (System.currentTimeMillis() - _sessionBeginInMs) / 1000.0f);
                }
            }
            log.info("[{}] ApoActor.close ended", _sessionId);
        } catch (Exception ex) {
            log.warn("[{}] ApoActor.close with exception: {}", _sessionId, ExceptionUtil.exception2detail(ex));
        }
    }

    private void generateRecordAndUpload() {
        if (null == _aiSetting) {
            log.warn("[{}]: [{}]-[{}]: generateRecordAndUpload _aiSetting is null ", _clientIp, _sessionId, _uuid);
            return;
        }

        final String path = _aiSetting.getAnswer_record_file();
        // eg: "answer_record_file": "rms://{uuid={uuid},url=xxxx,bucket=<bucketName>}<objectName>",
        final int braceBegin = path.indexOf('{');
        if (braceBegin == -1) {
            log.warn("[{}]: [{}]-[{}]: {} missing vars, ignore", _clientIp, _sessionId, _uuid, path);
            return;
        }
        final int braceEnd = path.lastIndexOf('}');
        if (braceEnd == -1) {
            log.warn("[{}]: [{}]-[{}]: {} missing vars, ignore", _clientIp, _sessionId, _uuid, path);
            return;
        }
        final String vars = path.substring(braceBegin + 1, braceEnd);

        final String bucketName = VarsUtil.extractValue(vars, "bucket");
        if (null == bucketName) {
            log.warn("[{}]: [{}]-[{}]: {} missing bucket field, ignore", _clientIp, _sessionId, _uuid, path);
            return;
        }

        final String objectName = path.substring(braceEnd + 1);

        // start to generate record file, REF: https://developer.aliyun.com/article/245440
        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
             final InputStream upsample_is = new ByteArrayListInputStream(_usBufs)) {
            final byte[] one_sample = new byte[2];
            InputStream downsample_is = null;

            // output wave header with 16K sample rate and 2 channels
            bos.write(WaveUtil.genWaveHeader(16000, 2));

            long currentInMs = _asrStartInMs.get();
            final AtomicReference<PlaybackSegment> ps = new AtomicReference<>(null), next_ps = new AtomicReference<>(null);

            int sample_count = 0;
            while (upsample_is.read(one_sample) == 2) {
                // read up stream data ok
                bos.write(one_sample);

                if (ps.get() == null) {
                    // init ps & next_ps if downstream pcm exist
                    fetchPS(ps, next_ps, currentInMs);
                }
                if (next_ps.get() != null && next_ps.get().timestamp == currentInMs) {
                    // current ps & next_ps overlapped
                    // ignore the rest of current ps, and using next_ps as new ps for recording
                    log.info("[{}]: [{}]-[{}]: current ps overlapped with next ps at timestamp: {}, using_next_ps_as_current",
                            _clientIp, _sessionId, _uuid, next_ps.get().timestamp);
                    fetchPS(ps, next_ps, currentInMs);
                    downsample_is = null;
                }
                if (downsample_is == null && ps.get() != null && ps.get().timestamp == currentInMs) {
                    log.info("[{}]: [{}]-[{}]: current ps {} match currentInMS",
                            _clientIp, _sessionId, _uuid, ps.get().timestamp);
                    downsample_is = new ByteArrayListInputStream(ps.get()._data);
                }
                boolean downsample_written = false;
                if (downsample_is != null) {
                    if (downsample_is.read(one_sample) == 2) {
                        bos.write(one_sample);
                        downsample_written = true;
                    } else {
                        // current ps written
                        downsample_is.close();
                        downsample_is = null;
                        ps.set(null);
                        next_ps.set(null);
                    }
                }
                if (!downsample_written) {
                    // silent data
                    one_sample[0] = (byte)0x00;
                    one_sample[1] = (byte)0x00;
                    bos.write(one_sample);
                }
                sample_count++;
                if (sample_count % 16 == 0) {
                    // 1 millisecond == 16 sample == 16 * 2 bytes = 32 bytes
                    currentInMs++;
                }
            }

            log.info("[{}]: [{}]-[{}] generateRecordAndUpload: total written {} samples, {} seconds",
                    _clientIp, _sessionId, _uuid, sample_count, (float)sample_count / 16000);

            bos.flush();
            _doSaveRecord.accept(new RecordContext(_sessionId, bucketName, objectName, new ByteArrayInputStream(bos.toByteArray())));
        } catch (IOException ex) {
            log.warn("[{}]: [{}]-[{}] generateRecordAndUpload: generate record stream error, detail: {}",
                    _clientIp, _sessionId, _uuid, ExceptionUtil.exception2detail(ex));
            throw new RuntimeException(ex);
        }
    }

    private void fetchPS(final AtomicReference<PlaybackSegment> ps, final AtomicReference<PlaybackSegment> next_ps, final long currentInMs) {
        if (!_dsBufs.isEmpty()) {
            ps.set(_dsBufs.remove(0));
            if (ps.get() != null) {
                log.info("[{}]: [{}]-[{}]: new current ps's start tm: {} / currentInMS: {}", _clientIp, _sessionId, _uuid, ps.get().timestamp, currentInMs);
            }
            if (!_dsBufs.isEmpty()) {
                // prefetch next ps
                next_ps.set(_dsBufs.get(0));
                if (next_ps.get() != null) {
                    log.info("[{}]: [{}]-[{}]: next current ps's start tm: {} / currentInMS: {}",
                            _clientIp, _sessionId, _uuid, next_ps.get().timestamp, currentInMs);
                }
            } else {
                next_ps.set(null);
            }
        }
    }

    private boolean doPlayback(final AIReplyVO replyVO) {
        final String newPlaybackId = UUID.randomUUID().toString();
        log.info("[{}]: [{}]-[{}]: doPlayback: {}", _clientIp, _sessionId, _uuid, replyVO);
        final Supplier<Runnable> playback = _reply2playback.apply(newPlaybackId, replyVO);
        if (playback == null) {
            log.info("[{}]: [{}]-[{}]: doPlayback: unknown reply: {}, ignore", _clientIp, _sessionId, _uuid, replyVO);
            return false;
        } else {
            final Runnable stopPlayback = _currentStopPlayback.getAndSet(null);
            if (stopPlayback != null) {
                stopPlayback.run();
            }
            _currentPlaybackId.set(newPlaybackId);
            _currentPlaybackPaused.set(false);
            _currentPlaybackDuration.set(()->0L);
            createPlaybackMemo(newPlaybackId,
                    replyVO.getAi_content_id(),
                    replyVO.getCancel_on_speak(),
                    replyVO.getHangup() == 1);
            _currentStopPlayback.set(playback.get());
            return true;
        }
    }

    private BiConsumer<String, Object> _sendEvent;

    private final AtomicBoolean _isUserAnswered = new AtomicBoolean(false);

    private final String _clientIp;
    private final String _uuid;
    private final String _tid;

    @Autowired
    private CallApi _callApi;

    @Autowired
    private ScriptApi _scriptApi;

    @Value("#{${esl.api.headers}}")
    private Map<String,String> _esl_headers;

    @Autowired(required = false)
    private EslApi _eslApi;

    private final Consumer<ApoActor> _doHangup;
    private final AtomicReference<AIReplyVO> _welcome = new AtomicReference<>(null);
    private AiSettingVO _aiSetting;

    private final AtomicReference<Runnable> _currentStopPlayback = new AtomicReference<>(null);
    private final AtomicReference<String> _currentPlaybackId = new AtomicReference<>(null);
    private final AtomicBoolean _currentPlaybackPaused = new AtomicBoolean(false);
    private final AtomicReference<Supplier<Long>> _currentPlaybackDuration = new AtomicReference<>(()->0L);

    private final AtomicLong _idleStartInMs = new AtomicLong(System.currentTimeMillis());
    private final AtomicBoolean _isUserSpeak = new AtomicBoolean(false);
    private final AtomicLong _currentSentenceBeginInMs = new AtomicLong(-1);

    private final List<byte[]> _usBufs = new ArrayList<>();

    @RequiredArgsConstructor
    private static class PlaybackSegment {
        final long timestamp;
        final List<byte[]> _data = new LinkedList<>();
    }
    private final AtomicReference<PlaybackSegment> _currentPS = new AtomicReference<>(null);
    private final List<PlaybackSegment> _dsBufs = new ArrayList<>();

    private final AtomicLong _asrStartInMs = new AtomicLong(0);
    private int _emptyUserSpeechCount = 0;

    private final Consumer<RecordContext> _doSaveRecord;

    private final long _sessionBeginInMs;
    private final AtomicInteger _playbackIdx = new AtomicInteger(0);
    private final ConcurrentMap<String, PlaybackMemo> _id2memo = new ConcurrentHashMap<>();

    private void createPlaybackMemo(final String playbackId,
                                    final Long contentId,
                                    final boolean cancelOnSpeak,
                                    final boolean hangup) {
        _id2memo.put(playbackId,
                PlaybackMemo.builder()
                        .playbackIdx(_playbackIdx.incrementAndGet())
                        .contentId(Long.toString(contentId))
                        .beginInMs(System.currentTimeMillis())
                        .cancelOnSpeak(cancelOnSpeak)
                        .hangup(hangup)
                        .sampleWhenCreate(Timer.start())
                        .build());
    }

    private PlaybackMemo memoFor(final String playbackId) {
        return _id2memo.get(playbackId);
    }

    @Builder
    @Data
    @ToString
    private static class PlaybackMemo {
        public final int playbackIdx;
        public final String contentId;
        public final boolean cancelOnSpeak;
        public final boolean hangup;
        public final Timer.Sample sampleWhenCreate;
        public long beginInMs;
        public long eventInMs;
    }
}
