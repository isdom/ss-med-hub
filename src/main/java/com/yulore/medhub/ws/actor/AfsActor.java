package com.yulore.medhub.ws.actor;

import com.yulore.medhub.api.AIReplyVO;
import com.yulore.medhub.api.ApiResponse;
import com.yulore.medhub.api.ScriptApi;
import com.yulore.medhub.service.ASRConsumer;
import com.yulore.medhub.service.ASROperator;
import com.yulore.medhub.service.ASRService;
import com.yulore.medhub.vo.*;
import com.yulore.medhub.vo.cmd.AFSPlaybackStarted;
import com.yulore.medhub.vo.cmd.AFSPlaybackStopped;
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
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
public class AfsActor {
    public interface Context {
        int localIdx();
        String uuid();
        String sessionId();
        String welcome();
        long answerInMss();
        Consumer<Runnable> runOn();
        BiFunction<AIReplyVO, Supplier<String>, String> reply2Rms();
        BiConsumer<String, Object> sendEvent();
    }

    public AfsActor(final Context ctx) {
        this.localIdx = ctx.localIdx();
        this.uuid = ctx.uuid();
        this.sessionId = ctx.sessionId();
        this.welcome = ctx.welcome();
        // TODO: assume answer timestamp as record start timestamp, maybe using real rst?
        this._recordStartInMs.set(ctx.answerInMss() / 1000L);
        this.runOn = ctx.runOn();
        this.reply2Rms = ctx.reply2Rms();
        this.sendEvent = ctx.sendEvent();
    }

    private final int localIdx;
    private final String uuid;
    private final String sessionId;
    private final String welcome;
    private final Consumer<Runnable> runOn;
    private final BiFunction<AIReplyVO, Supplier<String>, String> reply2Rms;
    private final BiConsumer<String, Object> sendEvent;

    @Autowired
    private ASRService _asrService;

    @Resource
    private ScriptApi _scriptApi;

    private final AtomicReference<ASROperator> opRef = new AtomicReference<>(null);
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public void startTranscription() {
        _asrService.startTranscription(new ASRConsumer() {
            @Override
            public void onSentenceBegin(PayloadSentenceBegin payload) {
            }

            @Override
            public void onTranscriptionResultChanged(PayloadTranscriptionResultChanged payload) {
                log.info("afs_io => onTranscriptionResultChanged: {}", payload);
            }

            @Override
            public void onSentenceEnd(final PayloadSentenceEnd payload) {
                runOn.accept(()->{
                    log.info("afs_io => onSentenceEnd: {}", payload);
                    whenASRSentenceEnd(payload);
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
                opRef.set(operator);
                if (isClosed.get()) {
                    // means close() method has been called
                    final var op = opRef.getAndSet(null);
                    if (op != null) { // means when close() called, operator !NOT! set yet
                        op.close();
                    }
                }
            }
        });
    }

    private int transmitCount = 0;
    private long transmitDelayPer50 = 0, handleCostPer50 = 0;
    public void transmit(final byte[] frame, final long fsReadFrameInMss, final long recvdInMs, final Timer td_timer, final Timer hc_timer) {
        if (!isClosed.get()) {
            final var operator = opRef.get();
            if (operator != null) {
                //final byte[] frame = new byte[buffer.remaining()];
                //buffer.get(frame);
                operator.transmit(frame);
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
            }
        }
    }

    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            final ASROperator operator = opRef.getAndSet(null);
            if (null != operator) {
                operator.close();
            }

            _id2memo.clear();
        }
    }

    public void checkIdle() {
        // TODO:
    }

    @PostConstruct
    private void playWelcome() {
        try {
            final ApiResponse<AIReplyVO> response =
                    _scriptApi.ai_reply(sessionId, welcome, null, 0, null, 0);
            log.info("[{}]: playWelcome: call ai_reply with {} => response: {}", sessionId, welcome, response);
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
            log.warn("[{}]: playWelcome: ai_reply error, hangup, detail: {}", sessionId, ExceptionUtil.exception2detail(ex));
        }
    }

    public void playbackStarted(final AFSPlaybackStarted vo) {
        log.info("afs_io({}) => playbackStarted: playback_id:{}/started delay: {} ms/full cost: {} ms",
                localIdx, vo.playback_id,
                (vo.eventInMss - vo.startInMss) / 1000L,
                System.currentTimeMillis() - vo.startInMss / 1000L);

        final PlaybackMemo playbackMemo = memoFor(vo.playback_id);
        // TODO: playbackMemo.sampleWhenCreate.stop(playback_timer);

        final long playbackStartedInMs = System.currentTimeMillis();
        playbackMemo.setBeginInMs(playbackStartedInMs);

        final String currentPlaybackId = _currentPlaybackId.get();
        if (currentPlaybackId != null) {
            if (currentPlaybackId.equals(vo.playback_id)) {
                _currentPlaybackDuration.set(()->System.currentTimeMillis() - playbackStartedInMs);
                log.info("[{}] playbackStarted => current playbackid: {} Matched",
                        sessionId, vo.playback_id);
            } else {
                log.info("[{}] playbackStarted => current playbackid: {} mismatch started playbackid: {}, ignore",
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
    }

    private void doHangup() {
        sendEvent.accept("Hangup", AFSHangupEvent.builder()
                .localIdx(localIdx)
                .hangupInMs(System.currentTimeMillis())
                .build());
    }

    private boolean doPlayback(final AIReplyVO replyVO) {
        if (replyVO.getVoiceMode() == null || replyVO.getAi_content_id() == null) {
            return false;
        }

        final String newPlaybackId = UUID.randomUUID().toString();
        final String ai_content_id = Long.toString(replyVO.getAi_content_id());
        log.info("[{}] doPlayback: {}", sessionId, replyVO);

        final long now = System.currentTimeMillis();
        final String file = reply2Rms.apply(replyVO,
                () -> String.format("vars_playback_id=%s,content_id=%s,vars_start_timestamp=%d,playback_idx=%d",
                        newPlaybackId, ai_content_id, now * 1000L, 0));

        if (file != null) {
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
                            .file(file)
                            .build()
            );
            log.info("[{}] doPlayback [{}] as {}", sessionId, file, newPlaybackId);
            return true;
        } else {
            return false;
        }
    }

    /* TODO:
    private void whenASRSentenceBegin(final PayloadSentenceBegin payload) {
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
    */

    private void whenASRSentenceEnd(final PayloadSentenceEnd payload) {
        final long sentenceEndInMs = System.currentTimeMillis();
        _isUserSpeak.set(false);
        _idleStartInMs.set(sentenceEndInMs);

        String userContentId = null;
        try {
            final boolean isAiSpeaking = isAiSpeaking();
            final String userSpeechText = payload.getResult();
            final String aiContentId = currentAiContentId();
            final int speakingDuration = currentSpeakingDuration();

            log.info("[{}] whenASRSentenceEnd: before ai_reply => speech:{}/is_speaking:{}/content_id:{}/speaking_duration:{} s",
                    sessionId, userSpeechText, isAiSpeaking, aiContentId, (float)speakingDuration / 1000.0f);
            final ApiResponse<AIReplyVO> response =
                    _scriptApi.ai_reply(sessionId, userSpeechText, null, isAiSpeaking ? 1 : 0, aiContentId, speakingDuration);
            log.info("[{}] whenASRSentenceEnd: ai_reply ({})", sessionId, response);
            if (response.getData() != null) {
                if (response.getData().getUser_content_id() != null) {
                    userContentId = response.getData().getUser_content_id().toString();
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

        {
            // report USER speak timing
            final long start_speak_timestamp = _asrStartedInMs.get() + payload.getBegin_time();
            final long stop_speak_timestamp = _asrStartedInMs.get() + payload.getTime();
            final long user_speak_duration = stop_speak_timestamp - start_speak_timestamp;

            final ApiResponse<Void> resp = _scriptApi.report_content(
                    sessionId,
                    userContentId,
                    payload.getIndex(),
                    "USER",
                    _recordStartInMs.get(),
                    start_speak_timestamp,
                    stop_speak_timestamp,
                    user_speak_duration);
            log.info("[{}] user report_content ({})'s response: {}", sessionId, userContentId, resp);
        }
        {
            // report ASR event timing
            // sentence_begin_event_time in Milliseconds
            final long begin_event_time = _currentSentenceBeginInMs.get() - _asrStartedInMs.get();

            // sentence_end_event_time in Milliseconds
            final long end_event_time = sentenceEndInMs - _asrStartedInMs.get();

            final ApiResponse<Void> resp = _scriptApi.report_asrtime(
                    sessionId,
                    userContentId,
                    payload.getIndex(),
                    begin_event_time,
                    end_event_time);
            log.info("[{}]: user report_asrtime ({})'s response: {}", sessionId, userContentId, resp);
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
    }

    private final AtomicInteger _playbackIdx = new AtomicInteger(0);
    private final ConcurrentMap<String, PlaybackMemo> _id2memo = new ConcurrentHashMap<>();

    private final AtomicReference<String> _currentPlaybackId = new AtomicReference<>(null);
    private final AtomicBoolean _currentPlaybackPaused = new AtomicBoolean(false);
    private final AtomicReference<Supplier<Long>> _currentPlaybackDuration = new AtomicReference<>(()->0L);

    private final AtomicLong _idleStartInMs = new AtomicLong(System.currentTimeMillis());
    private final AtomicBoolean _isUserSpeak = new AtomicBoolean(false);
    private final AtomicLong _asrStartedInMs = new AtomicLong(0);
    private final AtomicLong _recordStartInMs = new AtomicLong(-1);
    private final AtomicLong _currentSentenceBeginInMs = new AtomicLong(-1);
}
