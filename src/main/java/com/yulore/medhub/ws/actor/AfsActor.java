package com.yulore.medhub.ws.actor;

import com.yulore.medhub.api.AIReplyVO;
import com.yulore.medhub.api.ApiResponse;
import com.yulore.medhub.api.EslApi;
import com.yulore.medhub.api.ScriptApi;
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
import io.micrometer.core.instrument.Timer;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

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

    public interface Context {
        int localIdx();
        String uuid();
        String sessionId();
        String welcome();
        long answerInMss();
        int idleTimeout();
        Consumer<Runnable> runOn();
        Reply2Rms reply2rms();
        BiConsumer<String, Object> sendEvent();
        Executor esl();
    }

    public AfsActor(final Context ctx) {
        this.localIdx = ctx.localIdx();
        this.uuid = ctx.uuid();
        this.sessionId = ctx.sessionId();
        this.welcome = ctx.welcome();
        this._answerInMs = ctx.answerInMss() / 1000L;
        this._idleTimeout = ctx.idleTimeout();
        this.runOn = ctx.runOn();
        this.reply2rms = ctx.reply2rms();
        this.sendEvent = ctx.sendEvent();
        this._eslExecutor = ctx.esl();
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
    private final Consumer<Runnable> runOn;
    private final Reply2Rms reply2rms;
    private final BiConsumer<String, Object> sendEvent;

    @Autowired
    private ASRService _asrService;

    @Autowired
    private ScriptApi _scriptApi;

    @Value("#{${esl.api.headers}}")
    private Map<String,String> _esl_headers;

    @Autowired(required = false)
    private EslApi _eslApi;

    private final Executor _eslExecutor;

    private final AtomicReference<ASROperator> asrRef = new AtomicReference<>(null);
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public void startTranscription() {
        log.info("[{}] startTranscription with ScriptApi({})/EslApi({})", sessionId, _scriptApi, _eslApi);
        _asrService.startTranscription(new ASRConsumer() {
            @Override
            public void onSentenceBegin(PayloadSentenceBegin payload) {
                runOn.accept(()-> {
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
                runOn.accept(()->{
                    log.info("afs_io => onSentenceEnd: {}", payload);
                    whenASRSentenceEnd(payload, sentenceEndInMs);
                });
            }

            @Override
            public void onTranscriberFail() {
                log.warn("afs_io => onTranscriberFail");
            }
        }).whenComplete((operator, ex) -> {
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
        });
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

    public void checkIdle() {
        final long idleTime = System.currentTimeMillis() - _idleStartInMs.get();
        if (sessionId != null      // user answered
            && !_isUserSpeak.get()  // user not speak
            && !isAiSpeaking()      // AI not speak
            && _idleTimeout > 0
        ) {
            if (idleTime > _idleTimeout) {
                log.info("[{}] checkIdle: idle duration: {} ms >=: [{}] ms", sessionId, idleTime, _idleTimeout);
                try {
                    final ApiResponse<AIReplyVO> response =
                            _scriptApi.ai_reply(sessionId, null, idleTime, 0, null, 0);
                    log.info("[{}] checkIdle: ai_reply {}", sessionId, response);
                    if (response.getData() != null) {
                        if (doPlayback(response.getData())) {
                        } else if (response.getData().getHangup() == 1) {
                            doHangup();
                            log.info("[{}] checkIdle: hangup ({}) for ai_reply ({})", sessionId, sessionId, response.getData());
                        }
                    } else {
                        log.info("[{}] checkIdle: ai_reply's data is null, do_nothing", sessionId);
                    }
                } catch (final Exception ex) {
                    log.warn("[{}] checkIdle: ai_reply error, detail: {}", sessionId, ExceptionUtil.exception2detail(ex));
                }
            }
        }
        log.info("[{}] checkIdle: is_speaking: {}/is_playing: {}/idle duration: {} ms/idle_timeout: {} ms",
                sessionId, _isUserSpeak.get(), isAiSpeaking(), idleTime, _idleTimeout);
    }

    private void playWelcome() {
        try {
            final ApiResponse<AIReplyVO> response =
                    _scriptApi.ai_reply(sessionId, welcome, null, 0, null, 0);
            log.info("[{}] playWelcome: call ai_reply with {} => response: {}", sessionId, welcome, response);
            if (response.getData() != null) {
                if (!doPlayback(response.getData())) {
                    if (response.getData().getHangup() == 1) {
                        doHangup();
                        log.info("[{}] playWelcome: hangup ({}) for ai_reply ({})", sessionId, sessionId, response.getData());
                    }
                }
            } else {
                doHangup();
                log.warn("[{}] playWelcome: ai_reply({}), hangup", sessionId, response);
            }
        } catch (final Exception ex) {
            doHangup();
            log.warn("[{}] playWelcome: ai_reply error, hangup, detail: {}", sessionId, ExceptionUtil.exception2detail(ex));
        }
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

        String userContentId = null;
        String user_qa_id = null;
        try {
            final boolean isAiSpeaking = isAiSpeaking();
            final String userSpeechText = payload.getResult();
            final String aiContentId = currentAiContentId();
            final int speakingDuration = currentSpeakingDuration();

            if (userSpeechText == null || userSpeechText.isEmpty()) {
                _emptyUserSpeechCount++;
                log.warn("[{}] whenASRSentenceEnd: skip ai_reply => [{}] speech_is_empty, total empty count: {}",
                        sessionId, payload.getIndex(), _emptyUserSpeechCount);
                return;
            }

            log.info("[{}] whenASRSentenceEnd: before ai_reply => speech:{}/is_speaking:{}/content_id:{}/speaking_duration:{} s",
                    sessionId, userSpeechText, isAiSpeaking, aiContentId, (float)speakingDuration / 1000.0f);
            final ApiResponse<AIReplyVO> response =
                    _scriptApi.ai_reply(sessionId, userSpeechText, null, isAiSpeaking ? 1 : 0, aiContentId, speakingDuration);
            log.info("[{}] whenASRSentenceEnd: ai_reply ({})", sessionId, response);
            if (response.getData() != null) {
                if (response.getData().getUser_content_id() != null) {
                    userContentId = response.getData().getUser_content_id().toString();
                    user_qa_id = response.getData().getQa_id();
                }
                if (!doPlayback(response.getData()))  {
                    if (response.getData().getHangup() == 1) {
                        doHangup();
                        log.info("[{}] whenASRSentenceEnd : hangup ({}) for ai_reply ({})", sessionId, sessionId, response.getData());
                    }
                    if (isAiSpeaking && _currentPlaybackPaused.compareAndSet(true, false) ) {
                        sendEvent.accept("FSResumePlayback", null /*new PayloadFSChangePlayback(_uuid, _currentPlaybackId.get())*/);
                        log.info("[{}] whenASRSentenceEnd: resume_current ({}) for ai_reply ({}) without_new_ai_playback",
                                sessionId, _currentPlaybackId.get(), payload.getResult());
                    }
                }
            } else {
                log.info("[{}] whenASRSentenceEnd: ai_reply's data is null', do_nothing", sessionId);
            }
        } catch (final Exception ex) {
            log.warn("[{}] whenASRSentenceEnd: ai_reply error, detail: {}", sessionId, ExceptionUtil.exception2detail(ex));
        }

        final var content_id = userContentId;
        final var content_index = payload.getIndex() - _emptyUserSpeechCount;
        final var qa_id = user_qa_id;
        {
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
        {
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

        if (_eslApi != null && !payload.getResult().isEmpty() && payload.getResult().length() >=5) {
            final var startInMs = System.currentTimeMillis();
            _eslExecutor.execute(()->{
                final var resp = _eslApi.search_ref(_esl_headers, payload.getResult(), 0.5f);
                final long cost = System.currentTimeMillis() - startInMs;
                log.info("[{}]: {} => ESL Response: {}, cost {} ms", sessionId, payload.getResult(), resp, cost);
                if (resp.getResult() != null && resp.getResult().length > 0) {
                    final var ess = new ScriptApi.ExampleSentence[resp.getResult().length];
                    int idx = 0;
                    for (var hit : resp.getResult()) {
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
                            .content_id(content_id)
                            .content_index(content_index)
                            .qa_id(qa_id)
                            .es(ess)
                            .embedding_cost(resp.dev.embeddingDuration)
                            .db_cost(resp.dev.dbExecutionDuration)
                            .total_cost((int) cost)
                            .build();
                    final var resp2 = _scriptApi.report_es(req);
                    log.info("[{}]: report_es => req: {}/resp: {}", sessionId, req, resp2);
                }
            });
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

    private final long _answerInMs;
    //private final AtomicLong _recordStartInMs = new AtomicLong(-1);
    private final List<Consumer<ReportContext>> _pendingReports = new ArrayList<>();
    private final AtomicLong _currentSentenceBeginInMs = new AtomicLong(-1);
}
