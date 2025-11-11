package com.yulore.medhub.ws.actor;

import com.yulore.medhub.api.*;
import com.yulore.medhub.service.ASRConsumer;
import com.yulore.medhub.service.ASROperator;
import com.yulore.medhub.service.ASRService;
import com.yulore.medhub.vo.*;
import com.yulore.medhub.vo.cmd.AFSPlaybackStarted;
import com.yulore.medhub.vo.cmd.AFSPlaybackStopped;
import com.yulore.medhub.vo.cmd.AFSRecordStarted;
import com.yulore.medhub.vo.cmd.AFSRemoveLocalCommand;
import com.yulore.medhub.vo.event.AFSHangupEvent;
import com.yulore.medhub.vo.event.AFSStartPlaybackEvent;
import com.yulore.medhub.vo.event.AFSStopPlaybackEvent;
import com.yulore.util.ExceptionUtil;
import com.yulore.util.ExecutorStore;
import io.micrometer.core.instrument.Timer;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;

import static org.springframework.beans.factory.config.ConfigurableBeanFactory.SCOPE_PROTOTYPE;

@Slf4j
@Component
@Scope(SCOPE_PROTOTYPE)
public final class AfsActor {
    @ToString
    public static class RmsSource {
        public String file;
        public String local_key;
        public String local_vars;
    }
    public interface Reply2Rms extends BiFunction<AIReplyVO, Supplier<String>, RmsSource> {}
    public interface MatchEsl extends BiFunction<String, String, EslApi.EslResponse<EslApi.Hit>> {}
    public interface NDMUserSpeech extends Function<DialogApi.UserSpeechRequest, ApiResponse<DialogApi.UserSpeechResult>> {}

    public interface Context {
        int localIdx();
        String uuid();
        String sessionId();
        String welcome();
        long answerInMss();
        int idleTimeout();
        Executor executor();
        Reply2Rms reply2rms();
        BiConsumer<String, Object> sendEvent();
        MatchEsl matchEsl();
        NDMUserSpeech userSpeech();
    }

    public AfsActor(final Context ctx) {
        this.localIdx = ctx.localIdx();
        this.uuid = ctx.uuid();
        this.sessionId = ctx.sessionId();
        this.welcome = ctx.welcome();
        this._answerInMs = ctx.answerInMss() / 1000L;
        this._idleTimeout = ctx.idleTimeout();
        this.executor = ctx.executor();
        this.reply2rms = ctx.reply2rms();
        this.sendEvent = ctx.sendEvent();
        this._matchEsl = ctx.matchEsl();
        this._ndmUserSpeech = ctx.userSpeech();
    }

    public int localIdx() {
        return this.localIdx;
    }

    public String sessionId() {
        return this.sessionId;
    }

    private final int localIdx;
    private final String uuid;
    private final String sessionId;
    private final String welcome;
    private final Executor executor;
    private final Reply2Rms reply2rms;
    private final BiConsumer<String, Object> sendEvent;

    @Autowired
    private ExecutorStore executorStore;

    @Autowired
    private ASRService _asrService;

    @Autowired
    private ScriptApi _scriptApi;

    @Autowired
    private IntentConfig intentConfig;

    final private MatchEsl _matchEsl;
    final private NDMUserSpeech _ndmUserSpeech;

    private final AtomicReference<ASROperator> asrRef = new AtomicReference<>(null);
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicBoolean isWelcomePlayed = new AtomicBoolean(false);
    private boolean _use_esl = false;
    private String _esl_partition = null;

    private int _lastIterationIdx = 0;
    private final Map<Integer, String> _pendingIteration = new HashMap<>();

    public void start() {
        log.info("[{}] start with ScriptApi({})/matchEsl({})", sessionId, _scriptApi, _matchEsl);
        _asrService.startTranscription(new ASRConsumer() {
            @Override
            public void onSentenceBegin(PayloadSentenceBegin payload) {
                executor.execute(()-> {
                    log.info("afs_io => onSentenceBegin: {}", payload);
                    whenASRSentenceBegin(payload);
                });
            }

            @Override
            public void onTranscriptionResultChanged(PayloadTranscriptionResultChanged payload) {
                log.info("afs_io => onTranscriptionResultChanged: {}", payload);
            }

            @Override
            public void onSentenceEnd(final PayloadSentenceEnd payload) {
                final long sentenceEndInMs = System.currentTimeMillis();
                executor.execute(()->{
                    log.info("afs_io => onSentenceEnd: {}", payload);
                    whenASRSentenceEnd(payload, sentenceEndInMs);
                });
            }

            @Override
            public void onTranscriberFail(final Object reason) {
                log.warn("[{}] afs_io => onTranscriberFail: {}", sessionId,
                        reason != null ? reason.toString() : "(null)");
            }
        }).whenCompleteAsync(onStartTranscriptionComplete(), executor);
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
                    // afs session not closed
                    playWelcome();
                }
            }
        };
    }

    private int transmitCount = 0;
    private long transmitDelayPer50 = 0, handleCostPer50 = 0;
    public void transmit(final byte[] frame, final long fsReadFrameInMss, final long recvdInMs, final Timer td_timer, final Timer hc_timer) {
        if (!isClosed.get()) {
            final var operator = asrRef.get();
            if (operator != null) {
                try {
                    operator.transmit(frame);
                } catch (Exception ex) {
                    log.warn("[{}] asr_transmit_failed: {}", sessionId, ExceptionUtil.exception2detail(ex));
                }
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
            }
        }
    }

    public void close(final AFSRemoveLocalCommand removeLocalVO) {
        try {
            if (isClosed.compareAndSet(false, true)) {
                final ASROperator operator = asrRef.getAndSet(null);
                if (null != operator) {
                    operator.close();
                }

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

                    final var ctx = new ReportContext() {
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
                _id2memo.clear();
                _pendingReports.clear();
                _recordStartedVO = null;
            }
            log.info("[{}] AfsActor.close ended", sessionId);
        } catch (Exception ex) {
            log.warn("[{}] AfsActor.close with exception: {}", sessionId, ExceptionUtil.exception2detail(ex));
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

    private Supplier<ApiResponse<AIReplyVO>> callAiReplyWithWelcome(final String welcome) {
        return () -> {
            log.info("[{}] callAiReplyWithWelcome: before ai_reply => {}", sessionId, welcome);
            return _scriptApi.ai_reply(sessionId, null, welcome, null, 0, null, 0);
        };
    }

    private Supplier<ApiResponse<AIReplyVO>> callAiReplyWithIdleTime(final long idleTime) {
        return () -> _scriptApi.ai_reply(
                sessionId, null, null, idleTime, 0, null, 0);
    }

    private Supplier<ApiResponse<AIReplyVO>> callAiReplyWithSpeech(final String speechText, final int content_index) {
        return ()-> {
            final boolean isAiSpeaking = isAiSpeaking();
            final String aiContentId = currentAiContentId();
            final int speakingDuration = currentSpeakingDuration();
            log.info("[{}] before ai_reply => ({}) speech:{}/is_speaking:{}/content_id:{}/speaking_duration:{} s",
                    sessionId, content_index, speechText, isAiSpeaking, aiContentId, (float)speakingDuration / 1000.0f);
            return _scriptApi.ai_reply(sessionId, content_index, speechText, null, isAiSpeaking ? 1 : 0, aiContentId, speakingDuration);
        };
    }

    private Supplier<ApiResponse<ScriptApi.Text2IntentResult>> callSpeech2Intent(final String speechText, final int content_index) {
        return ()-> {
            log.info("[{}] before ai_t2i => ({}) speech:{}", sessionId, content_index, speechText);
            return _scriptApi.ai_t2i(ScriptApi.Text2IntentRequest.builder()
                    .speechText(speechText)
                    .speechIdx(content_index)
                    .sessionId(sessionId).build());
        };
    }

    private Supplier<ApiResponse<AIReplyVO>> callIntent2Reply(
            final String traceId,
            final String intent,
            final String speechText,
            final int content_index) {
        return ()-> {
            final boolean isAiSpeaking = isAiSpeaking();
            final String aiContentId = currentAiContentId();
            final int speakingDuration = currentSpeakingDuration();
            log.info("[{}] before ai_i2r => ({}) speech:{}/traceId:{}/intent:{}/is_speaking:{}/content_id:{}/speaking_duration:{} s",
                    sessionId, content_index, speechText, traceId, intent,
                    isAiSpeaking, aiContentId, (float)speakingDuration / 1000.0f);
            return _scriptApi.ai_i2r(ScriptApi.Intent2ReplyRequest.builder()
                    .sessionId(sessionId)
                    .traceId(traceId)
                    .intent(intent)
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
                log.warn("[{}] interact failed for {}, retry", sessionId, ExceptionUtil.exception2detail(ex));
                return buildExecution.get();
            } else {
                return CompletableFuture.failedStage(ex);
            }
        };
    }

    private void playWelcome() {
        final var getReply = callAiReplyWithWelcome(welcome);
        interactAsync(getReply)
                .exceptionallyCompose(handleRetryable(()->interactAsync(getReply)))
                .whenCompleteAsync(handleWelcomeReply(), executor)
        ;
    }

    public void checkIdle() {
        final long idleTime = System.currentTimeMillis() - _idleStartInMs.get();
        if (isWelcomePlayed.get()
            && !_isUserSpeak.get()  // user not speak
            && !isAiSpeaking()      // AI not speak
            && _idleTimeout > 0
        ) {
            if (idleTime > _idleTimeout) {
                if (hasPendingIteration()) {
                    log.info("[{}] checkIdle: has_pending_iteration, skip", sessionId);
                } else {
                    final var iterationIdx = addIteration("checkIdle");
                    log.info("[{}] checkIdle: iteration: {} / idle duration: {} ms >=: [{}] ms", sessionId, iterationIdx, idleTime, _idleTimeout);
                    interactAsync(callAiReplyWithIdleTime(idleTime))
                            .whenCompleteAsync((response, ex) -> {
                                completeIteration(iterationIdx);
                                if (ex != null) {
                                    log.warn("[{}] checkIdle: ai_reply error, detail: {}", sessionId, ExceptionUtil.exception2detail(ex));
                                } else if (!isCurrentIteration(iterationIdx)) {
                                    log.warn("[{}] checkIdle: iteration mismatch({} != {}), skip", sessionId, iterationIdx, _lastIterationIdx);
                                } else {
                                    log.info("[{}] checkIdle: ai_reply {}", sessionId, response);
                                    if (response.getData() != null) {
                                        if (!doPlayback(response.getData())) {
                                            if (response.getData().getHangup() == 1) {
                                                doHangup();
                                                log.info("[{}] checkIdle: hangup ({}) for ai_reply ({})", sessionId, sessionId, response.getData());
                                            }
                                        }
                                    } else {
                                        log.info("[{}] checkIdle: ai_reply's data is null, do_nothing", sessionId);
                                    }
                                }
                            }, executor)
                    ;
                }
            }
        }
        log.info("[{}] checkIdle: welcome: {}/is_speaking: {}/is_playing: {}/idle duration: {} ms/idle_timeout: {} ms",
                sessionId, isWelcomePlayed.get(), _isUserSpeak.get(), isAiSpeaking(), idleTime, _idleTimeout);
    }

    private BiConsumer<ApiResponse<AIReplyVO>, Throwable>
    handleWelcomeReply() {
        return (response, ex) -> {
            isWelcomePlayed.set(true);
            if (ex != null) {
                doHangup();
                log.warn("[{}] handleWelcomeReply: hangup for ai_reply error, detail: {}", sessionId, ExceptionUtil.exception2detail(ex));
            } else {
                log.info("[{}] handleWelcomeReply: call ai_reply with {} => response: {}", sessionId, welcome, response);
                if (response.getData() != null) {
                    _use_esl = response.getData().getUse_esl() != null ? response.getData().getUse_esl() : false;
                    _esl_partition = response.getData().getEsl_partition();
                    log.info("[{}] handleWelcomeReply: using_esl_status {}", sessionId, _use_esl);
                    if (!doPlayback(response.getData())) {
                        if (response.getData().getHangup() == 1) {
                            doHangup();
                            log.info("[{}] handleWelcomeReply: hangup ({}) for ai_reply ({})", sessionId, sessionId, response.getData());
                        }
                    }
                } else {
                    doHangup();
                    log.warn("[{}] playWelcome: ai_reply({}), hangup", sessionId, response);
                }
            }
        };
    }

    public void playbackStarted(final AFSPlaybackStarted vo) {
        final long playbackStartedInMs = System.currentTimeMillis();
        log.info("afs_io({}) => playbackStarted: playback_id:{}/started delay: {} ms/full cost: {} ms",
                localIdx, vo.playback_id,
                (vo.eventInMss - vo.startInMss) / 1000L,
                playbackStartedInMs - vo.startInMss / 1000L);

        final PlaybackMemo playbackMemo = memoFor(vo.playback_id);
        // TODO: playbackMemo.sampleWhenCreate.stop(playback_timer);

        playbackMemo.setBeginInMs(playbackStartedInMs);
        playbackMemo.setEventInMs(vo.eventInMss / 1000L);

        final String currentPlaybackId = _currentPlaybackId.get();
        if (currentPlaybackId != null) {
            if (currentPlaybackId.equals(vo.playback_id)) {
                _currentPlaybackDuration.set(()->System.currentTimeMillis() - playbackStartedInMs);
                log.info("[{}] playbackStarted => current playbackId: {} Matched", sessionId, vo.playback_id);
            } else {
                log.info("[{}] playbackStarted => current playbackId: {} mismatch started playbackId: {}, ignore",
                        sessionId, currentPlaybackId, vo.playback_id);
            }
        } else {
            log.warn("[{}] currentPlaybackId is null BUT playbackStarted with: {}", sessionId, vo.playback_id);
        }
    }

    public void playbackStopped(final AFSPlaybackStopped vo) {
        if (_currentPlaybackId.get() != null
            && vo.playback_id != null
            && vo.playback_id.equals(_currentPlaybackId.get()) ) {
            _currentPlaybackId.set(null);
            _currentPlaybackDuration.set(()->0L);
            _idleStartInMs.set(System.currentTimeMillis());
            log.info("[{}] playbackStopped: current playback_id matched:{}, clear current PlaybackId", sessionId, vo.playback_id);
            if (memoFor(vo.playback_id).isHangup()) {
                // hangup call
                doHangup();
            }
        } else {
            log.info("[{}] stopped playback_id:{} is !NOT! current playback_id:{}, ignored", sessionId, vo.playback_id, _currentPlaybackId.get());
        }
        // build _scriptApi.report_content for AI
        _pendingReports.add(buildAIReport(vo.playback_id, vo.playbackMs));
    }

    private Consumer<ReportContext> buildAIReport(final String playback_id, final int playback_ms) {
        return ctx -> {
            final var memo = memoFor(playback_id);
            final var org_diff = Math.max(memo.eventInMs - ctx.rms_rst(), 0);
            final var scaled_start = ctx.rms_rst() + (long) (org_diff * ctx.scale_factor());

            final var resp = _scriptApi.report_content(
                    sessionId,
                    memo.contentId,
                    memo.playbackIdx,
                    "AI",
                    ctx.rms_rst(),
                    scaled_start,
                    scaled_start + playback_ms,
                    playback_ms);
            log.info("[{}] ai_report_content ({}) => org_diff:{} / scaled_diff: {} / playback_ms: {}",
                    sessionId,
                    memo.playbackIdx,
                    org_diff,
                    scaled_start - ctx.rms_rst(),
                    playback_ms);
        };
    }

    public void recordStarted(final AFSRecordStarted vo) {
        _recordStartedVO = vo;
        //_recordStartInMs.set(vo.recordStartInMss / 1000L);
        //log.info("[{}] record diff from answer: {} ms", sessionId, _recordStartInMs.get() - _answerInMs);
    }

    private void doHangup() {
        sendEvent.accept("Hangup", AFSHangupEvent.builder()
                .localIdx(localIdx)
                .hangupInMs(System.currentTimeMillis())
                .build());
    }

    static final String RMS_VARS = "fs_uuid=%s,session_id=%s,vars_playback_id=%s,content_id=%s," +
                                    "vars_start_timestamp=%d,playback_idx=%d,local_idx=%d";
    private boolean doPlayback(final AIReplyVO replyVO) {
        if (replyVO.getVoiceMode() == null || replyVO.getAi_content_id() == null) {
            log.info("[{}] doPlayback without_voice_mode_or_ai_content_id: [{}/{}], skip", sessionId, replyVO.getVoiceMode(), replyVO.getAi_content_id());
            return false;
        }

        final String newPlaybackId = UUID.randomUUID().toString();
        final String ai_content_id = Long.toString(replyVO.getAi_content_id());
        log.info("[{}] doPlayback: {}", sessionId, replyVO);

        final long now = System.currentTimeMillis();
        final var src = reply2rms.apply(replyVO,
                () -> String.format(RMS_VARS,
                        uuid,
                        sessionId,
                        newPlaybackId,
                        ai_content_id,
                        now * 1000L,
                        _playbackIdx.get() + 1,
                        localIdx)
        );

        if (src.file != null) {
            final String prevPlaybackId = _currentPlaybackId.getAndSet(null);
            if (prevPlaybackId != null) {
                sendEvent.accept("StopPlayback",
                        AFSStopPlaybackEvent.builder()
                                .localIdx(localIdx)
                                .playback_id(prevPlaybackId)
                                .stopEventInMs(now)
                                .build()
                );
            }
            _currentPlaybackId.set(newPlaybackId);
            _currentPlaybackPaused.set(false);
            _currentPlaybackDuration.set(()->0L);
            createPlaybackMemo(newPlaybackId,
                    replyVO.getAi_content_id(),
                    replyVO.getCancel_on_speak(),
                    replyVO.getHangup() == 1);
            sendEvent.accept("StartPlayback",
                    AFSStartPlaybackEvent.builder()
                            .localIdx(localIdx)
                            .playback_id(newPlaybackId)
                            .content_id(ai_content_id)
                            .file(src.file)
                            .local_key(src.local_key)
                            .local_vars(src.local_vars)
                            .build()
            );
            if (src.local_key == null) {
                log.info("[{}] doPlayback [{}] as {}", sessionId, src.file, newPlaybackId);
            } else {
                log.info("[{}] doPlayback [{}] with_local_key:{} and vars:{} as {}", sessionId, src.file, src.local_key, src.local_vars, newPlaybackId);
            }
            return true;
        } else {
            return false;
        }
    }

    private void whenASRSentenceBegin(final PayloadSentenceBegin payload) {
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
    }

    private boolean isAICancelOnSpeak() {
        final String playbackId = _currentPlaybackId.get();
        if (playbackId != null) {
            return memoFor(playbackId).isCancelOnSpeak();
        } else {
            return false;
        }
    }

    private void whenASRSentenceEnd(final PayloadSentenceEnd payload, final long sentenceEndInMs) {
        _isUserSpeak.set(false);
        _idleStartInMs.set(sentenceEndInMs);

        final String speechText = payload.getResult();
        if (speechText == null || speechText.isEmpty()) {
            _emptyUserSpeechCount++;
            log.info("[{}] whenASRSentenceEnd: skip ai_reply => [{}] speech_is_empty, total empty count: {}",
                    sessionId, payload.getIndex(), _emptyUserSpeechCount);
        } else {
            if (!isWelcomePlayed.get()) {
                // TODO: welcome_not_play_yet
                log.warn("[{}] whenASRSentenceEnd: welcome_not_play_yet, skip", sessionId);
                return;
            }
            final var content_index = payload.getIndex() - _emptyUserSpeechCount;
            _userContentIndex.set(content_index);
            final var iterationIdx = addIteration(speechText);
            log.info("[{}] whenASRSentenceEnd: addIteration => {}", sessionId, iterationIdx);
            speech2reply(speechText, content_index)
            .whenCompleteAsync(handleAiReply(content_index, payload), executor)
            .whenComplete((ignored, ex)-> {
                completeIteration(iterationIdx);
                if (ex == null) {
                    log.info("[{}] whenASRSentenceEnd: completeIteration({})", sessionId, iterationIdx);
                } else {
                    log.warn("[{}] whenASRSentenceEnd: completeIteration_with_ex({}, {})",
                            sessionId, iterationIdx, ExceptionUtil.exception2detail(ex));
                }
            })
            .whenComplete(reportUserSpeech(content_index, payload, sentenceEndInMs))
            ;
        }
    }

    private CompletionStage<ApiResponse<AIReplyVO>> speech2reply(final String speechText, final int content_index) {
        return _use_esl ? scriptAndEslMixed(speechText, content_index) : scriptOnly(speechText, content_index);
    }

    private CompletionStage<ApiResponse<AIReplyVO>> scriptOnly(final String speechText, final int content_index) {
        final var getReply = callAiReplyWithSpeech(speechText, content_index);
        return interactAsync(getReply).exceptionallyCompose(handleRetryable(()->interactAsync(getReply)));
    }

    private CompletionStage<ApiResponse<AIReplyVO>> scriptAndEslMixed(final String speechText, final int content_index) {
        final var esl_cost = new AtomicLong(0);
        final AtomicReference<EslApi.EslResponse<EslApi.Hit>> esl_resp_ref = new AtomicReference<>(null);
        final AtomicReference<String> t2i_intent_ref = new AtomicReference<>(null);
        final var getIntent = callSpeech2Intent(speechText, content_index);

        return interactAsync(getIntent).exceptionallyCompose(handleRetryable(()->interactAsync(getIntent)))
                .thenCombine(interactAsync(callMatchEsl(speechText, content_index, esl_cost))
                                .exceptionally(handleEslSearchException()), Pair::of)
                .thenComposeAsync(script_and_esl->{
                    final var t2i_resp = script_and_esl.getLeft();
                    final var esl_resp = script_and_esl.getRight();
                    log.info("[{}] whenASRSentenceEnd script_and_esl done with {}\n[{}] ai_t2i resp: {}\n[{}] esl_search resp: {}",
                            sessionId, intentConfig, sessionId, t2i_resp, sessionId, esl_resp);
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
                    final var getReply = callIntent2Reply(t2i_result.getTraceId(), final_intent, speechText, content_index);
                    return interactAsync(getReply).exceptionallyCompose(handleRetryable(()->interactAsync(getReply)));
                }, executor)
                .whenCompleteAsync(reportEsl(t2i_intent_ref, esl_resp_ref, content_index, esl_cost), executorStore.apply("feign"));
    }

    private Function<Throwable, EslApi.EslResponse<EslApi.Hit>> handleEslSearchException() {
        return ex -> {
            log.warn("[{}] call_esl_text_failed: {}", sessionId, ExceptionUtil.exception2detail(ex));
            return EslApi.emptyResponse();
        };
    }

    private Supplier<EslApi.EslResponse<EslApi.Hit>> callMatchEsl(
            final String speechText,
            final int content_index,
            final AtomicLong cost) {
        return ()-> {
            log.info("[{}] before match_esl: ({}) speech:{} partition:{}", sessionId, content_index, speechText, _esl_partition);
            final var startInMs = System.currentTimeMillis();
            try {
                return _matchEsl.apply(speechText, _esl_partition);
            } finally {
                cost.set(System.currentTimeMillis() - startInMs);
                log.info("[{}] after match_esl: ({}) speech:{} partition:{} => cost {} ms",
                        sessionId, content_index, speechText, _esl_partition, cost.longValue());
            }
        };
    }

    private BiConsumer<ApiResponse<AIReplyVO>, Throwable>
    handleAiReply(final int content_index,
                  final PayloadSentenceEnd payload) {
        return (ai_resp, ex) -> {
            if (ex != null) {
                log.warn("[{}] onAiReply: ai_reply error, detail: {}", sessionId, ExceptionUtil.exception2detail(ex));
                return;
            }

            if (_userContentIndex.get() != content_index) {
                log.warn("[{}] onAiReply: ai_reply's user_content_idx:{} != current_user_content_idx:{}, skip response: {}",
                        sessionId, content_index, _userContentIndex.get(), ai_resp);
                return;
            }

            final boolean isAiSpeaking = isAiSpeaking();

            log.info("[{}] onAiReply: ai_reply ({})", sessionId, ai_resp);
            if (ai_resp.getData() != null) {
                if (!doPlayback(ai_resp.getData()))  {
                    if (ai_resp.getData().getHangup() == 1) {
                        doHangup();
                        log.info("[{}] onAiReply : hangup ({}) for ai_reply ({})", sessionId, sessionId, ai_resp.getData());
                    }
                    if (isAiSpeaking && _currentPlaybackPaused.compareAndSet(true, false) ) {
                        sendEvent.accept("FSResumePlayback", null /*new PayloadFSChangePlayback(_uuid, _currentPlaybackId.get())*/);
                        log.info("[{}] onAiReply: resume_current ({}) for ai_reply ({}) without_new_ai_playback",
                                sessionId, _currentPlaybackId.get(), payload.getResult());
                    }
                }
            } else {
                log.info("[{}] onAiReply: ai_reply's data is null', do_nothing", sessionId);
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
                log.warn("[{}] onAiReply: ai_reply({}) => user_content_id_is_null", sessionId, payload.getResult());
            }
            reportUserContent(content_index, payload, sentenceEndInMs, userContentId);
            reportAsrTime(content_index, sentenceEndInMs, userContentId);
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
            log.info("[{}]: ESL Response: {}, cost {} ms", sessionId, esl_resp, cost.longValue());
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
                        .session_id(sessionId)
                        .content_id(userContentId)
                        .content_index(content_index)
                        .qa_id(t2i_intent)
                        .es(ess)
                        .embedding_cost(esl_resp.dev.embeddingDuration)
                        .db_cost(esl_resp.dev.dbExecutionDuration)
                        .total_cost(cost.intValue())
                        .build();
                final var resp2 = _scriptApi.report_es(req);
                log.info("[{}]: report_es => req: {}/resp: {}", sessionId, req, resp2);
            }
        };
    }

    private void reportUserContent(final int content_index,
                                   final PayloadSentenceEnd payload,
                                   final long sentenceEndInMs,
                                   final String content_id) {
        // build USER speak timing report
        final long start_speak_timestamp = _asrStartedInMs.get() + payload.getBegin_time();
        //final long start_speak_timestamp = _currentSentenceBeginInMs.get();
        final long stop_speak_timestamp = _asrStartedInMs.get() + payload.getTime();
        final long user_speak_duration = stop_speak_timestamp - start_speak_timestamp;
        // final long user_speak_duration = sentenceEndInMs - start_speak_timestamp;
        log.info("[{}] USER report_content({}) diff, (sentence_begin-start) = {} ms,  (sentence_end-stop) = {} ms",
                sessionId, content_index, _currentSentenceBeginInMs.get() - start_speak_timestamp, sentenceEndInMs - stop_speak_timestamp);

        final Consumer<ReportContext> doReport = ctx-> {
            final ApiResponse<Void> resp = _scriptApi.report_content(
                    sessionId,
                    content_id,
                    content_index,
                    "USER",
                    ctx.event_rst(),
                    start_speak_timestamp,
                    stop_speak_timestamp,
                    user_speak_duration);
            log.info("[{}] user_report_content ({}): rst:{} / asr_start:{} / asr_stop: {}",
                    sessionId,
                    content_index,
                    ctx.event_rst(),
                    start_speak_timestamp,
                    stop_speak_timestamp);
        };
        _pendingReports.add(doReport);
    }

    private void reportAsrTime(final int content_index,
                               final long sentenceEndInMs,
                               final String content_id) {
        // report ASR event timing
        // sentence_begin_event_time in Milliseconds
        final long begin_event_time = _currentSentenceBeginInMs.get() - _asrStartedInMs.get();

        // sentence_end_event_time in Milliseconds
        final long end_event_time = sentenceEndInMs - _asrStartedInMs.get();

        final Consumer<ReportContext> doReport = ctx-> {
            final ApiResponse<Void> resp = _scriptApi.report_asrtime(
                    sessionId,
                    content_id,
                    content_index,
                    begin_event_time,
                    end_event_time);
            log.info("[{}] user_report_asrtime ({})'s response: {}", sessionId, content_id, resp);
        };
        _pendingReports.add(doReport);
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

    interface ReportContext {
        long event_rst();
        long rms_rst();
        float scale_factor();
    }

    private AFSRecordStarted _recordStartedVO;
    private final AtomicInteger _playbackIdx = new AtomicInteger(0);
    private final ConcurrentMap<String, PlaybackMemo> _id2memo = new ConcurrentHashMap<>();

    private final AtomicReference<String> _currentPlaybackId = new AtomicReference<>(null);
    private final AtomicBoolean _currentPlaybackPaused = new AtomicBoolean(false);
    private final AtomicReference<Supplier<Long>> _currentPlaybackDuration = new AtomicReference<>(()->0L);

    private final int _idleTimeout;

    private final AtomicLong _idleStartInMs = new AtomicLong(System.currentTimeMillis());
    private final AtomicBoolean _isUserSpeak = new AtomicBoolean(false);
    private final AtomicLong _asrStartedInMs = new AtomicLong(0);
    private int _emptyUserSpeechCount = 0;
    private final AtomicInteger _userContentIndex = new AtomicInteger(0);

    private final long _answerInMs;
    //private final AtomicLong _recordStartInMs = new AtomicLong(-1);
    private final List<Consumer<ReportContext>> _pendingReports = new ArrayList<>();
    private final AtomicLong _currentSentenceBeginInMs = new AtomicLong(-1);
}
