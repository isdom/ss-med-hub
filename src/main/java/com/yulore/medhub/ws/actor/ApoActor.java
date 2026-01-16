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
import com.yulore.util.*;
import io.micrometer.core.instrument.Timer;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
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
import java.util.concurrent.*;
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
    private ExecutorStore executorStore;

    @Autowired
    private ASRService _asrService;

    private final AtomicReference<ASROperator> asrRef = new AtomicReference<>(null);

    private int _lastIterationIdx = 0;
    private final Map<Integer, String> _pendingIteration = new HashMap<>();

    private final Executor _executor;

    public interface Reply2Playback extends BiFunction<String, AIReplyVO, Supplier<Runnable>> {}
    // public interface NDMUserSpeech extends Function<DialogApi.UserSpeechRequest, ApiResponse<DialogApi.UserSpeechResult>> {}
    public interface NDMSpeech2Intent extends Function<DialogApi.ClassifySpeechRequest, DialogApi.EsMatchResult> {}

    private Reply2Playback _reply2playback;

    public interface Context {
        int actorIdx();
        String clientIp();
        String uuid();
        String tid();
        // long answerInMss();
        Executor executor();
        Consumer<ApoActor> doHangup();
        Consumer<RecordContext> saveRecord();
        Consumer<ApoActor> callStarted();
        // NDMUserSpeech userSpeech();
        NDMSpeech2Intent speech2intent();
    }

    public ApoActor(final Context ctx) {
        _actorIdx = ctx.actorIdx();
        _sessionBeginInMs = System.currentTimeMillis();

        _clientIp = ctx.clientIp();
        _uuid = ctx.uuid();
        _tid = ctx.tid();
        _executor = ctx.executor();
        _doHangup = ctx.doHangup();
        _doSaveRecord = ctx.saveRecord();
        _callStarted = ctx.callStarted();
        // _ndmUserSpeech = ctx.userSpeech();
        _ndmSpeech2Intent = ctx.speech2intent();
        if (Strings.isNullOrEmpty(_uuid) || Strings.isNullOrEmpty(_tid)) {
            log.warn("[{}]: ApoActor_ctor error for uuid:{}/tid:{}, abort apply_session.", _clientIp, _sessionId, _uuid);
            return;
        }
    }

    public boolean start(final ScheduledFuture<?> checkFuture) {
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
            return true;
        } catch (Exception ex) {
            log.warn("[{}]: [{}]-[{}]: failed for ApoActor.start, detail: {}", _clientIp, _sessionId, _uuid,
                    ExceptionUtil.exception2detail(ex));
            return false;
        }
    }

    public void notifyMockAnswer() {
        _executor.execute(()-> {
            if (!isClosed.get()) {
                if (_isUserAnswered.get()) {
                    log.info("[{}]: [{}]-[{}]: notifyUserAnswer has called already, ignore notifyMockAnswer!", _clientIp, _sessionId, _uuid);
                    return;
                }
                if (Strings.isNullOrEmpty(_uuid) || Strings.isNullOrEmpty(_tid)) {
                    log.warn("[{}]/[{}]: doMockAnswer error for uuid:{}/tid:{}, abort mock_answer.", _clientIp, _sessionId, _uuid, _tid);
                    return;
                }

                final var answerTime = System.currentTimeMillis();
                interactAsync(()->{
                    log.info("[{}]: [{}]-[{}]: before_mockAnswer for tid:{}", _clientIp, _sessionId, _uuid, _tid);
                    return _callApi.mock_answer(CallApi.MockAnswerRequest.builder()
                        .sessionId(_sessionId)
                        .uuid(_uuid)
                        .tid(_tid)
                        .answerTime(answerTime)
                        .build());
                })
                .whenCompleteAsync((response, ex) -> {
                    if (ex != null) {
                        log.warn("[{}]: [{}]-[{}]: failed for callApi.mock_answer, detail: {}",
                                _clientIp, _sessionId, _uuid, ExceptionUtil.exception2detail(ex));
                    } else {
                        log.info("[{}]: [{}]-[{}]: mockAnswer: tid:{} => response: {}", _clientIp, _sessionId, _uuid, _tid, response);
                        saveAndPlayWelcome(response);
                    }
                }, _executor);
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

        final var answerTime = System.currentTimeMillis();
        interactAsync(()->{
            log.info("[{}]: [{}]-[{}]: before_userAnswer: {}", _clientIp, _sessionId, _uuid, vo);
            return _callApi.user_answer(CallApi.UserAnswerRequest.builder()
                .sessionId(_sessionId)
                .kid(vo.kid)
                .tid(vo.tid)
                .realName(vo.realName)
                .genderStr(vo.genderStr)
                .aesMobile(vo.aesMobile)
                .answerTime(answerTime)
                .build());
        })
        .whenCompleteAsync((response, ex) -> {
            if (ex != null) {
                log.warn("[{}]: [{}]-[{}]: failed for callApi.user_answer, detail: {}",
                        _clientIp, _sessionId, _uuid, ExceptionUtil.exception2detail(ex));
            } else {
                log.info("[{}]: [{}]-[{}]: userAnswer: {} => response: {}", _clientIp, _sessionId, _uuid, vo, response);
                saveAndPlayWelcome(response);
            }
        }, _executor);
    }

    private void saveAndPlayWelcome(final ApiResponse<UserAnswerVO> response) {
        if (null != response.getData()) {
            _use_esl = response.getData().getUse_esl() != null ? response.getData().getUse_esl() : false;
            _esl_partition = response.getData().getEsl_partition();
            _welcome.set(response.getData());
            // _lastReply.set(response.getData());
            log.info("[{}]: [{}]-[{}]: saveAndPlayWelcome: _use_esl:{}/_esl_partition:{}/_welcome:{}",
                    _clientIp, _sessionId, _uuid, _use_esl, _esl_partition, _welcome.get());
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
                _executor.execute(()-> {
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
                _executor.execute(()->{
                    log.info("apo_io => onSentenceEnd: {}", payload);
                    whenASRSentenceEnd(payload, sentenceEndInMs);
                });
            }

            @Override
            public void onTranscriberFail(Object reason) {
                log.warn("[{}] apo_io => onTranscriberFail: {}", _sessionId,
                        reason != null ? reason.toString() : "(null)");
            }
        }).whenCompleteAsync(onStartTranscriptionComplete(), _executor);
    }

    private BiConsumer<ASROperator, Throwable> onStartTranscriptionComplete() {
        return (operator, ex) -> {
            if (ex != null) {
                log.warn("startTranscription failed", ex);
            } else {
                asrRef.set(operator);
                if (isClosed.get()) {
                    // means close() method has been called
                    final var op = asrRef.getAndSet(null);
                    if (op != null) { // means when close() called, operator !NOT! set yet
                        op.close();
                    }
                } else {
                    // apo session not closed
                    tryPlayWelcome();
                }
            }
        };
    }

    private boolean isAiSpeaking() {
        return _currentPlaybackId.get() != null;
    }

    public void checkIdle() {
        _executor.execute(()-> {
            final long idleTime = System.currentTimeMillis() - _idleStartInMs.get();
            boolean isAiSpeaking = isAiSpeaking();
            if (_isUserAnswered.get()  // user answered
                && _reply2playback != null  // playback ws has connected
                && !_isUserSpeak.get()  // user not speak
                && !isAiSpeaking        // AI not speak
                && _aiSetting != null
                ) {
                if (idleTime > _aiSetting.getIdle_timeout()) {
                    if (hasPendingIteration()) {
                        log.info("[{}] checkIdle: has_pending_iteration, skip", _sessionId);
                    } else {
                        final var iterationIdx = addIteration("checkIdle");
                        log.info("[{}]: [{}]-[{}]: checkIdle: iteration: {} / idle duration: {} ms >=: [{}] ms",
                                _clientIp, _sessionId, _uuid, iterationIdx, idleTime, _aiSetting.getIdle_timeout());
                        interactAsync(callAiReplyWithIdleTime(idleTime))
                        .whenCompleteAsync((response, ex) -> {
                            completeIteration(iterationIdx);
                            if (ex != null) {
                                log.warn("[{}] checkIdle: ai_reply error, detail: {}", _sessionId, ExceptionUtil.exception2detail(ex));
                            } else if (!isCurrentIteration(iterationIdx)) {
                                log.warn("[{}] checkIdle: iteration mismatch({} != {}), skip", _sessionId, iterationIdx, _lastIterationIdx);
                            } else {
                                log.info("[{}] checkIdle: ai_reply {}", _sessionId, response);
                                if (response.getData() != null) {
                                    if (!doPlayback(response.getData())) {
                                        log.info("[{}]: [{}]-[{}]: checkIdle: ai_reply {}, !NOT! doPlayback", _clientIp, _sessionId, _uuid, response);
                                        if (response.getData().getHangup() == 1) {
                                            // hangup call
                                            _doHangup.accept(this);
                                            log.info("[{}] checkIdle: hangup ({}) for ai_reply ({})", _sessionId, _sessionId, response.getData());
                                        }
                                    }
                                } else {
                                    log.info("[{}] checkIdle: ai_reply's data is null, do_nothing", _sessionId);
                                }
                            }
                        }, _executor)
                        ;
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

        _isUserSpeak.set(false);
        _idleStartInMs.set(sentenceEndInMs);
        if (_sessionId != null && _reply2playback != null && _aiSetting != null) {
            final String speechText = payload.getResult();
            // playback ws has connected && _aiSetting is valid
            if (speechText == null || speechText.isEmpty()) {
                _emptyUserSpeechCount++;
                log.warn("[{}] whenASRSentenceEnd: skip ai_reply => [{}] speech_is_empty, total empty count: {}",
                        _sessionId, payload.getIndex(), _emptyUserSpeechCount);
            } else {
                final var content_index = payload.getIndex() - _emptyUserSpeechCount;
                _userContentIndex.set(content_index);
                final var iterationIdx = addIteration(speechText);
                log.info("[{}] whenASRSentenceEnd: addIteration => {}", _sessionId, iterationIdx);
                final AtomicReference<DialogApi.EsMatchResult> emrRef = new AtomicReference<>();
                //speech2reply(speechText, content_index)
                (_isNjd ? callAndNdm(speechText, content_index, emrRef)
                        : scriptAndNdm(speechText, content_index, emrRef))
                .whenCompleteAsync(handleAiReply(content_index, payload), _executor)
                .whenComplete((ignored, ex) -> {
                    completeIteration(iterationIdx);
                    if (ex == null) {
                        log.info("[{}] whenASRSentenceEnd: completeIteration({})", _sessionId, iterationIdx);
                    } else {
                        log.warn("[{}] whenASRSentenceEnd: completeIteration_with_ex({}, {})",
                                _sessionId, iterationIdx, ExceptionUtil.exception2detail(ex));
                    }
                })
                .whenComplete(reportUserSpeech(content_index, payload, sentenceEndInMs))
                .whenComplete(reportToNdm(emrRef))
                ;
            /*
            final String userContentId = interactWithScriptEngine(payload, content_index);
            reportUserContent(payload, userContentId);
            reportAsrTime(payload, sentenceEndInMs, userContentId);
             */
            }
        } else {
            log.warn("[{}]: [{}]-[{}]: whenASRSentenceEnd but sessionId is null or playback not ready or aiSetting not ready => (reply2playback: {}), (aiSetting: {})",
                    _clientIp, _sessionId, _uuid, _reply2playback, _aiSetting);
        }
    }

    private String interactWithScriptEngine(final PayloadSentenceEnd payload, final int content_index) {
        final boolean isAiSpeaking = isAiSpeaking();
        String userContentId = null;
        try {
            final String userSpeechText = payload.getResult();
            final String aiContentId = currentAiContentId();
            final int speakingDuration = currentSpeakingDuration();
            log.info("[{}]: [{}]-[{}]: ai_reply: speech:{}/is_speaking:{}/content_id:{}/speaking_duration:{} s",
                    _clientIp, _sessionId, _uuid, userSpeechText, isAiSpeaking, aiContentId, (float)speakingDuration / 1000.0f);
            final ApiResponse<AIReplyVO> response =
                    _scriptApi.ai_reply(_sessionId, payload.getIndex(), userSpeechText, null, isAiSpeaking ? 1 : 0, aiContentId, speakingDuration);

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
                        log.info("[{}]: [{}]-[{}]: interactWithScriptEngine: resume_current ({}) for ai_reply ({}) without_new_ai_playback",
                                _clientIp, _sessionId, _uuid, _currentPlaybackId.get(), payload.getResult());
                    }
                }
            } else {
                log.info("[{}]: [{}]-[{}]: interactWithScriptEngine: ai_reply {}, do_nothing", _clientIp, _sessionId, _uuid, response);
            }
        } catch (final Exception ex) {
            log.warn("[{}]: [{}]-[{}]: interactWithScriptEngine: ai_reply error, detail: {}", _clientIp, _sessionId, _uuid, ExceptionUtil.exception2detail(ex));
        }
        return userContentId;
    }

    /*
    private CompletionStage<ApiResponse<AIReplyVO>> speech2reply(
            final String speechText,
            final int content_index) {
        return _use_esl ? scriptAndEslMixed(speechText, content_index) : scriptOnly(speechText, content_index);
    }

    private CompletionStage<ApiResponse<AIReplyVO>> scriptOnly(final String speechText, final int content_index) {
        final var getReply = callAiReplyWithSpeech(speechText, content_index);
        return interactAsync(getReply).exceptionallyCompose(handleRetryable(()->interactAsync(getReply)));
    }
    */

    private CompletionStage<ApiResponse<AIReplyVO>> callAndNdm(
            final String speechText,
            final int content_index,
            final AtomicReference<DialogApi.EsMatchResult> emrRef) {
        final var esl_cost = new AtomicLong(0);

        return interactAsync(callNdmS2I(speechText, content_index, esl_cost))
                .thenComposeAsync(emr->{
                    emrRef.set(emr);
                    log.info("[{}] callAndNdm done with ndm_s2i resp: {}", _sessionId, emr);
                    final var getReply = callIntent2Reply(
                            emr.getIntents(),
                            speechText,
                            content_index);
                    return interactAsync(getReply).exceptionallyCompose(handleRetryable(()->interactAsync(getReply)));
                }, _executor);
    }

    private Supplier<ApiResponse<AIReplyVO>> callIntent2Reply(
            final Integer[] sysIntents,
            final String speechText,
            final int content_index) {
        return ()-> {
            final boolean isAiSpeaking = isAiSpeaking();
            final String aiContentId = currentAiContentId();
            final int speakingDuration = currentSpeakingDuration();
            log.info("[{}] before call_ai_i2r => ({}) speech:{}/intent:{}/is_speaking:{}/content_id:{}/speaking_duration:{} s",
                    _sessionId, content_index, speechText, sysIntents,
                    isAiSpeaking, aiContentId, (float)speakingDuration / 1000.0f);
            return _callApi.ai_i2r(Intent2ReplyRequest.builder()
                    .sessionId(_sessionId)
                    .sysIntents(sysIntents)
                    .speechIdx(content_index)
                    .speechText(speechText)
                    .isSpeaking(isAiSpeaking ? 1 : 0)
                    .speakingContentId(aiContentId != null ? Long.parseLong(aiContentId) : null)
                    .speakingDurationMs(speakingDuration)
                    .build());
        };
    }

    private CompletionStage<ApiResponse<AIReplyVO>> scriptAndNdm(
            final String speechText,
            final int content_index,
            final AtomicReference<DialogApi.EsMatchResult> emrRef) {
        final var esl_cost = new AtomicLong(0);
        final var scriptS2I = callScriptS2I(speechText, content_index);

        return interactAsync(scriptS2I).exceptionallyCompose(handleRetryable(()->interactAsync(scriptS2I)))
                .thenCombine(interactAsync(callNdmS2I(speechText, content_index, esl_cost)), Pair::of)
                .thenComposeAsync(script_and_ndm->{
                    final var t2i_resp = script_and_ndm.getLeft();
                    emrRef.set(script_and_ndm.getRight());
                    log.info("[{}] scriptAndNdm done with intent_config:{} / ai_t2i resp: {} / ndm_s2i resp: {}",
                            _sessionId, intentConfig, t2i_resp, script_and_ndm.getRight());
                    final String traceId = (t2i_resp != null && t2i_resp.getData() != null) ? t2i_resp.getData().getTraceId() : null;
                    final AtomicReference<Integer[]> sysIntentsRef = new AtomicReference<>(null);
                    final String ndm_intent = decideNdmIntent(script_and_ndm.getRight(), t2i_resp, sysIntentsRef);
                    final var getReply = scriptIntent2Reply(traceId, ndm_intent, sysIntentsRef.get(), speechText, content_index);
                    return interactAsync(getReply).exceptionallyCompose(handleRetryable(()->interactAsync(getReply)));
                }, _executor);
    }

    private String decideNdmIntent(final DialogApi.EsMatchResult emr,
                                   final ApiResponse<ScriptApi.Text2IntentResult> t2i_resp,
                                   final AtomicReference<Integer[]> sysIntentsRef) {
        if (t2i_resp != null && t2i_resp.getData() != null) {
            final var t2i_result = t2i_resp.getData();
            if (t2i_result.getIntentCode() != null
                && (intentConfig.getRing0().contains(t2i_result.getIntentCode())
                || t2i_result.getIntentCode().startsWith(intentConfig.getPrefix()))
            ) {
                // high priority intent, like phone's AI assistant
                // using t2i's intent
                return null;
            }
        }
        if (emr.getIntents() != null) {
            sysIntentsRef.set(emr.getIntents());
        }
        return emr.getOldIntent();
    }

    private Supplier<DialogApi.EsMatchResult> callNdmS2I(
            final String speechText,
            final int content_index,
            final AtomicLong cost) {
        return ()-> {
            log.info("[{}]: [{}]-[{}]: before speech2intent: ({}) speech:{} esl:{}",
                    _clientIp, _sessionId, _uuid, content_index, speechText, _ndm_esl);
            final var startInMs = System.currentTimeMillis();
            try {
                final var lastReply = _lastReply.get();
                return _ndmSpeech2Intent.apply(DialogApi.ClassifySpeechRequest.builder()
                        .esl(_ndm_esl)
                        .sessionId(_sessionId)
                        .botId(0)
                        .nodeId(0L)
                        .scriptText(lastReply != null ? lastReply.getScript_text() : null)
                        .speechText(speechText)
                        .build());
            } finally {
                cost.set(System.currentTimeMillis() - startInMs);
                log.info("[{}]: [{}]-[{}]: after speech2intent: ({}) speech:{} esl:{} => cost {} ms",
                        _clientIp, _sessionId, _uuid, content_index, speechText, _ndm_esl, cost.longValue());
            }
        };
    }

    /*
    private CompletionStage<ApiResponse<AIReplyVO>> scriptAndEslMixed(final String speechText, final int content_index) {
        final var esl_cost = new AtomicLong(0);
        final AtomicReference<EslApi.EslResponse<EslApi.Hit>> esl_resp_ref = new AtomicReference<>(null);
        final AtomicReference<String> t2i_intent_ref = new AtomicReference<>(null);
        final var getIntent = callScriptS2I(speechText, content_index);

        return interactAsync(getIntent).exceptionallyCompose(handleRetryable(()->interactAsync(getIntent)))
                .thenCombine(interactAsync(callMatchEsl(speechText, content_index, esl_cost))
                        .exceptionally(handleEslSearchException()), Pair::of)
                .thenComposeAsync(script_and_esl->{
                    final var t2i_resp = script_and_esl.getLeft();
                    final var esl_resp = script_and_esl.getRight();
                    log.info("[{}] scriptAndEslMixed script_and_esl done with {}\nai_t2i resp: {}\nesl_search resp: {}",
                            _sessionId, intentConfig, t2i_resp, esl_resp);
                    esl_resp_ref.set(esl_resp);
                    String final_intent = null;
                    String esl_intent = null;
                    var t2i_result = new ScriptApi.Text2IntentResult();
                    if (esl_resp.result != null && esl_resp.result.length > 0) {
                        // hit esl
                        esl_intent = esl_resp.result[0].es.intentionCode;
                    }
                    if (t2i_resp != null && t2i_resp.getData() != null) {
                        t2i_result = t2i_resp.getData();
                        t2i_intent_ref.set(t2i_result.getIntentCode());
                    }

                    if (t2i_result.getIntentCode() != null
                            && (intentConfig.getRing0().contains(t2i_result.getIntentCode())
                            || t2i_result.getIntentCode().startsWith(intentConfig.getPrefix()))
                    ) {
                        // using t2i's intent
                        final_intent = t2i_result.getIntentCode();
                    } else if (esl_intent != null) {
                        final_intent = esl_intent;
                    } else {
                        final_intent = t2i_result.getIntentCode();
                    }
                    final var getReply = scriptIntent2Reply(t2i_result.getTraceId(), final_intent, null, speechText, content_index);
                    return interactAsync(getReply).exceptionallyCompose(handleRetryable(()->interactAsync(getReply)));
                }, _executor)
                .whenCompleteAsync(reportEsl(t2i_intent_ref, esl_resp_ref, content_index, esl_cost), executorStore.apply("feign"));
    }

    private Function<Throwable, EslApi.EslResponse<EslApi.Hit>> handleEslSearchException() {
        return ex -> {
            log.warn("[{}] call_esl_text_failed: {}", _sessionId, ExceptionUtil.exception2detail(ex));
            return EslApi.emptyResponse();
        };
    }

    private Supplier<EslApi.EslResponse<EslApi.Hit>> callMatchEsl(
            final String speechText,
            final int content_index,
            final AtomicLong cost) {
        return ()-> {
            log.info("[{}]: [{}]-[{}]: before match_esl: ({}) speech:{} partition:{}",
                    _clientIp, _sessionId, _uuid, content_index, speechText, _esl_partition);
            final var startInMs = System.currentTimeMillis();
            try {
                return _matchEsl.apply(speechText, _esl_partition);
            } finally {
                cost.set(System.currentTimeMillis() - startInMs);
                log.info("[{}]: [{}]-[{}]: after match_esl: ({}) speech:{} partition:{} => cost {} ms",
                        _clientIp, _sessionId, _uuid, content_index, speechText, _esl_partition, cost.longValue());
            }
        };
    }
    */

    private BiConsumer<ApiResponse<AIReplyVO>, Throwable>
    handleAiReply(final int content_index,
                  final PayloadSentenceEnd payload) {
        return (ai_resp, ex) -> {
            if (ex != null) {
                log.warn("[{}] onAiReply: ai_reply error, detail: {}", _sessionId, ExceptionUtil.exception2detail(ex));
                return;
            }

            if (_userContentIndex.get() != content_index) {
                log.warn("[{}] onAiReply: ai_reply's user_content_idx:{} != current_user_content_idx:{}, skip response: {}",
                        _sessionId, content_index, _userContentIndex.get(), ai_resp);
                return;
            }

            final boolean isAiSpeaking = isAiSpeaking();

            log.info("[{}] onAiReply: ai_reply ({})", _sessionId, ai_resp);
            if (ai_resp.getData() != null) {
                if (!doPlayback(ai_resp.getData()))  {
                    if (ai_resp.getData().getHangup() == 1) {
                        _doHangup.accept(this);
                        log.info("[{}] onAiReply : hangup ({}) for ai_reply ({})", _sessionId, _sessionId, ai_resp.getData());
                    }
                    if (isAiSpeaking && _currentPlaybackPaused.compareAndSet(true, false) ) {
                        _sendEvent.accept("PCMResumePlayback", new PayloadPCMEvent(_currentPlaybackId.get(), ""));
                        log.info("[{}]: [{}]-[{}]: handleAiReply: resume_current ({}) for ai_reply ({}) without_new_ai_playback",
                                _clientIp, _sessionId, _uuid, _currentPlaybackId.get(), payload.getResult());
                    }
                }
            } else {
                log.info("[{}] onAiReply: ai_reply's data is null', do_nothing", _sessionId);
            }
        };
    }

    private BiConsumer<ApiResponse<AIReplyVO>, Throwable>
    reportUserSpeech(final int content_index,
                     final PayloadSentenceEnd payload,
                     final long sentenceEndInMs) {
        return (ai_resp, ex) -> {
            final var userContentId = (ai_resp != null && ai_resp.getData() != null && ai_resp.getData().getUser_content_id() != null)
                    ? ai_resp.getData().getUser_content_id().toString() : null;
            if (userContentId == null) {
                log.warn("[{}] reportUserSpeech: ai_reply({}) => user_content_id_is_null", _sessionId, payload.getResult());
            }
            reportUserContent(content_index, payload, sentenceEndInMs, userContentId);
            reportAsrTime(content_index, sentenceEndInMs, userContentId);
        };
    }

    private BiConsumer<ApiResponse<AIReplyVO>, Throwable>
    reportToNdm(final AtomicReference<DialogApi.EsMatchResult> emrRef) {
        return (ai_resp, ex) -> {
            final var contentId =
                    (ai_resp != null && ai_resp.getData() != null)
                    ? ai_resp.getData().getUser_content_id()
                    : null;
            final var emr = emrRef.get();
            if (emr != null && emr.contexts != null && contentId != null) {
                for (final var ctx : emr.contexts) {
                    ctx.setSessionId(_sessionId);
                    ctx.setContentId(contentId);
                }
                log.info("[{}] before reportToNdm: {}", _sessionId, emr.contexts.length);
                final var begin  = System.currentTimeMillis();
                try {
                    final var resp = _dialogApi.report_s2i(emr.contexts);
                } finally {
                    log.info("[{}] after reportToNdm: cost {} ms", _sessionId, (System.currentTimeMillis() - begin));
                }
            }
            if (contentId == null) {
                log.info("[{}] skip reportToNdm with {}, for user_content_id_is_null", _sessionId, emrRef.get());
            }
            /*
            if (ai_resp != null && ai_resp.getData() != null) {
                final var last = _lastReply.getAndSet(ai_resp.getData());
                if (last != null) {
                    final var request = DialogApi.UserSpeechRequest.builder()
                            .sessionId(_sessionId)
                            .botId(last.getBot_id())
                            .nodeId(last.getNode_id())
                            .qa_id(last.getQa_id())
                            .userContentId(ai_resp.getData().getUser_content_id())
                            .speechText(payload.getResult())
                            .build();
                    final var resp = _ndmUserSpeech.apply(request);
                    log.info("ndmUserSpeech's: req:{} => resp:{}", request, resp);
                } else {
                    log.info("[{}]: ndmUserSpeech's: last_reply_is_null", _sessionId);
                }
            }
            */
        };
    }

    private BiConsumer<ApiResponse<AIReplyVO>, Throwable>
    reportEsl(final AtomicReference<String> t2i_intent_ref,
              final AtomicReference<EslApi.EslResponse<EslApi.Hit>> esl_resp_ref,
              final int content_index,
              final AtomicLong cost) {
        return (ai_resp, ex) -> {
            final var userContentId = (ai_resp != null && ai_resp.getData() != null && ai_resp.getData().getUser_content_id() != null)
                    ? ai_resp.getData().getUser_content_id().toString() : null;

            final var t2i_intent = t2i_intent_ref.get();
            final var esl_resp = esl_resp_ref.get();
            log.info("[{}]: ESL Response: {}, cost {} ms", _sessionId, esl_resp, cost.longValue());
            if (esl_resp.getResult() != null && esl_resp.getResult().length > 0) {
                final var ess = new ScriptApi.ExampleSentence[esl_resp.getResult().length];
                int idx = 0;
                for (var hit : esl_resp.getResult()) {
                    ess[idx] = ScriptApi.ExampleSentence.builder()
                            .index(idx+1)
                            .id(hit.es.id)
                            .confidence(hit.confidence)
                            .intentionCode(hit.es.intentionCode)
                            .intentionName(hit.es.intentionName)
                            .text(hit.es.text)
                            .build();
                    idx++;
                }
                final var req = ScriptApi.ESRequest.builder()
                        .session_id(_sessionId)
                        .content_id(userContentId)
                        .content_index(content_index)
                        .qa_id(t2i_intent)
                        .es(ess)
                        .embedding_cost(esl_resp.dev.embeddingDuration)
                        .db_cost(esl_resp.dev.dbExecutionDuration)
                        .total_cost(cost.intValue())
                        .build();
                final var resp2 = _scriptApi.report_es(req);
                log.info("[{}]: report_es => req: {}/resp: {}", _sessionId, req, resp2);
            }
        };
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

    private void reportUserContent(final int content_index,
                                   final PayloadSentenceEnd payload,
                                   final long sentenceEndInMs,
                                   final String content_id) {
        // report USER speak timing
        // ASR-Sentence-Begin-Time in Milliseconds
        final long start_speak_timestamp = _asrStartInMs.get() + payload.getBegin_time();
        // ASR-Sentence-End-Time in Milliseconds
        final long stop_speak_timestamp = _asrStartInMs.get() + payload.getTime();
        final long user_speak_duration = stop_speak_timestamp - start_speak_timestamp;

        final ApiResponse<Void> resp = _scriptApi.report_content(
                _sessionId,
                content_id,
                content_index,
                "USER",
                _asrStartInMs.get(),
                start_speak_timestamp,
                stop_speak_timestamp,
                user_speak_duration);
        log.info("[{}]: [{}]-[{}]: user_report_content(content_id:{}/idx:{}/asr_start:{}/start_speak:{}/stop_speak:{}/speak_duration:{} s)'s resp: {}",
                _clientIp,
                _sessionId,
                _uuid,
                content_id,
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

    private void reportAsrTime(final int content_index,
                               final long sentenceEndInMs,
                               final String content_id) {
        // report ASR event timing
        // sentence_begin_event_time in Milliseconds
        final long begin_event_time = _currentSentenceBeginInMs.get() - _asrStartInMs.get();

        // sentence_end_event_time in Milliseconds
        final long end_event_time = sentenceEndInMs - _asrStartInMs.get();

        final ApiResponse<Void> resp = _scriptApi.report_asrtime(
                _sessionId,
                content_id,
                content_index,
                begin_event_time,
                end_event_time);
        log.info("[{}]: user_report_asrtime({})'s resp: {}", _sessionId, content_id, resp);
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

    private int addIteration(final String name) {
        _lastIterationIdx++;
        _pendingIteration.put(_lastIterationIdx, name);
        return _lastIterationIdx;
    }

    private void completeIteration(final int iterationIdx) {
        _pendingIteration.remove(iterationIdx);
    }

    private boolean isCurrentIteration(final int iterationIdx) {
        return _lastIterationIdx == iterationIdx;
    }

    private boolean hasPendingIteration() {
        return !_pendingIteration.isEmpty();
    }

    private Supplier<ApiResponse<AIReplyVO>> callAiReplyWithIdleTime(final long idleTime) {
        return _isNjd
            ?  () -> _callApi.ai_i2r(
                    Intent2ReplyRequest.builder()
                    .sessionId(_sessionId)
                    .isSpeaking(0)
                    .idleTime(idleTime)
                    .build())
            :() -> _scriptApi.ai_reply(
                    _sessionId,
                    null,
                    null,
                    idleTime, 0, null, 0)
            ;
    }

    private Supplier<ApiResponse<AIReplyVO>> callAiReplyWithSpeech(final String speechText, final int content_index) {
        return ()-> {
            final boolean isAiSpeaking = isAiSpeaking();
            final String aiContentId = currentAiContentId();
            final int speakingDuration = currentSpeakingDuration();
            log.info("[{}]: [{}]-[{}]: before ai_reply => ({}) speech:{}/is_speaking:{}/content_id:{}/speaking_duration:{} s",
                    _clientIp, _sessionId, _uuid, content_index, speechText, isAiSpeaking, aiContentId, (float)speakingDuration / 1000.0f);
            return _scriptApi.ai_reply(_sessionId, content_index, speechText, null, isAiSpeaking ? 1 : 0, aiContentId, speakingDuration);
        };
    }

    private Supplier<ApiResponse<ScriptApi.Text2IntentResult>> callScriptS2I(final String speechText, final int content_index) {
        return ()-> {
            log.info("[{}]: [{}]-[{}]: before ai_t2i => ({}) speech:{}", _clientIp, _sessionId, _uuid, content_index, speechText);
            return _scriptApi.ai_t2i(ScriptApi.Text2IntentRequest.builder()
                    .speechText(speechText)
                    .speechIdx(content_index)
                    .sessionId(_sessionId).build());
        };
    }

    private Supplier<ApiResponse<AIReplyVO>> scriptIntent2Reply(
            final String traceId,
            final String intent,
            final Integer[] sysIntents,
            final String speechText,
            final int content_index) {
        return ()-> {
            final boolean isAiSpeaking = isAiSpeaking();
            final String aiContentId = currentAiContentId();
            final int speakingDuration = currentSpeakingDuration();
            log.info("[{}] before ai_i2r => ({}) speech:{}/traceId:{}/intent:{}/is_speaking:{}/content_id:{}/speaking_duration:{} s",
                    _sessionId, content_index, speechText, traceId, intent,
                    isAiSpeaking, aiContentId, (float)speakingDuration / 1000.0f);
            return _scriptApi.ai_i2r(Intent2ReplyRequest.builder()
                    .sessionId(_sessionId)
                    .traceId(traceId)
                    .intent(intent)
                    .sysIntents(sysIntents)
                    .speechIdx(content_index)
                    .speechText(speechText)
                    .isSpeaking(isAiSpeaking ? 1 : 0)
                    .speakingContentId(aiContentId != null ? Long.parseLong(aiContentId) : null)
                    .speakingDurationMs(speakingDuration)
                    .build());
        };
    }

    private  <T> CompletionStage<T> interactAsync(final Supplier<T> getResponse) {
        return CompletableFuture.supplyAsync(getResponse, executorStore.apply("feign"));
    }

    private <T> Function<Throwable, CompletionStage<T>> handleRetryable(final Supplier<CompletionStage<T>> buildExecution) {
        return ex -> {
            if ((ex instanceof CompletionException)
                    && (ex.getCause() instanceof feign.RetryableException)) {
                log.warn("[{}] interact failed for {}, retry", _sessionId, ExceptionUtil.exception2detail(ex));
                return buildExecution.get();
            } else {
                return CompletableFuture.failedStage(ex);
            }
        };
    }

    private boolean doPlayback(final AIReplyVO replyVO) {
        final String newPlaybackId = UUID.randomUUID().toString();
        log.info("[{}]: [{}]-[{}]: doPlayback: {}", _clientIp, _sessionId, _uuid, replyVO);
        final Supplier<Runnable> playback = _reply2playback.apply(newPlaybackId, replyVO);
        if (playback == null) {
            log.info("[{}]: [{}]-[{}]: doPlayback: unknown reply: {}, ignore", _clientIp, _sessionId, _uuid, replyVO);
            return false;
        } else {
            try {
                final Runnable stopPlayback = _currentStopPlayback.getAndSet(null);
                if (stopPlayback != null) {
                    stopPlayback.run();
                }
                _lastReply.set(replyVO);
                _currentPlaybackId.set(newPlaybackId);
                _currentPlaybackPaused.set(false);
                _currentPlaybackDuration.set(() -> 0L);
                createPlaybackMemo(newPlaybackId,
                        replyVO.getAi_content_id(),
                        replyVO.getCancel_on_speak() != null ? replyVO.getCancel_on_speak() : false,
                        replyVO.getHangup() == 1);
                _currentStopPlayback.set(playback.get());
            } catch (Exception ex) {
                log.warn("exception when doPlayback, detail: {}", ExceptionUtil.exception2detail(ex));
            }
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

    @Autowired
    private DialogApi _dialogApi;

    @Autowired
    private IntentConfig intentConfig;

    //final private MatchEsl _matchEsl;

    final private AtomicReference<AIReplyVO> _lastReply = new AtomicReference<>(null);
    // final private NDMUserSpeech _ndmUserSpeech;
    private final NDMSpeech2Intent _ndmSpeech2Intent;

    @Value("${dialog.esl}")
    private String _ndm_esl;

    @Value("${dm.is_njd}")
    private boolean _isNjd;

    private boolean _use_esl = false;
    private String _esl_partition = null;

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
    private final AtomicInteger _userContentIndex = new AtomicInteger(0);

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
