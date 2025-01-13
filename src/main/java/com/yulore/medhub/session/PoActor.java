package com.yulore.medhub.session;

import com.alibaba.fastjson.JSON;
import com.alibaba.nls.client.protocol.SampleRateEnum;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriber;
import com.mgnt.utils.StringUnicodeEncoderDecoder;
import com.yulore.medhub.api.*;
import com.yulore.medhub.stream.VarsUtil;
import com.yulore.medhub.vo.*;
import com.yulore.util.ByteArrayListInputStream;
import com.yulore.util.ExceptionUtil;
import com.yulore.util.WaveUtil;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;

/**
 * Phone Operator Actor
 */
@ToString
@Slf4j
public class PoActor extends ASRActor {

    public record RecordContext(String sessionId, String bucketName, String objectName, InputStream content) {
    }

    public record PlaybackContext(String playbackId, String path, String contentId) {
    }

    @Builder
    @Data
    @ToString
    private static class PlaybackMemo {
        final int playbackIdx;
        final String contentId;
        final boolean cancelOnSpeak;
        final boolean hangup;
        long beginInMs;
    }

    public PoActor(final String uuid,
                   final String tid,
                   final CallApi callApi,
                   final ScriptApi scriptApi,
                   final Consumer<PoActor> doHangup,
                   final String bucket,
                   final String wavPath,
                   final Consumer<RecordContext> saveRecord,
                   final Consumer<String> callStarted) {
        _uuid = uuid;
        _tid = tid;
        _scriptApi = scriptApi;
        _callApi = callApi;
        _doHangup = doHangup;
        _bucket = bucket;
        _wavPath = wavPath;
        _doSaveRecord = saveRecord;
        try {
            final ApiResponse<ApplySessionVO> response = _callApi.apply_session(CallApi.ApplySessionRequest.builder()
                            .uuid(_uuid)
                            .tid(_tid)
                            .build());
            _sessionId = response.getData().getSessionId();
            log.info("[{}]-[{}]: apply_session => response: {}", _sessionId, _uuid, response);
            _callSessions.put(_sessionId, this);
            callStarted.accept(_sessionId);
        } catch (Exception ex) {
            log.warn("failed for callApi.apply_session, detail: {}", ex.toString());
        }
    }

    public void notifyUserAnswer(final HubCommandVO cmd) {
        if (!_isUserAnswered.compareAndSet(false, true)) {
            log.warn("[{}]-[{}]: notifyUserAnswer called already, ignore!", _sessionId, _uuid);
            return;
        }
        try {
            final String kid = cmd.getPayload() != null ? cmd.getPayload().get("kid") : "";
            final String tid = cmd.getPayload() != null ? cmd.getPayload().get("tid") : "";
            final String realName = cmd.getPayload() != null ? cmd.getPayload().get("realName") : "";
            final String aesMobile = cmd.getPayload() != null ? cmd.getPayload().get("aesMobile") : "";
            final String genderStr = cmd.getPayload() != null ? cmd.getPayload().get("genderStr") : "";

            final ApiResponse<UserAnswerVO> response = _callApi.user_answer(CallApi.UserAnswerRequest.builder()
                            .sessionId(_sessionId)
                            .kid(kid)
                            .tid(tid)
                            .realName(realName)
                            .genderStr(genderStr)
                            .aesMobile(aesMobile)
                            .answerTime(System.currentTimeMillis())
                            .build());
            log.info("[{}]-[{}]: userAnswer: kid:{}/tid:{}/realName:{}/gender:{}/aesMobile:{} => response: {}",
                    _sessionId, _uuid, kid, tid, realName, genderStr, aesMobile, response);

            _welcome.set(response.getData());
            _aiSetting = response.getData().getAiSetting();
            tryPlayWelcome();
        } catch (Exception ex) {
            log.warn("failed for callApi.user_answer, detail: {}", ex.toString());
        }
    }

    public void attachPlaybackWs(final Function<PlaybackContext, Runnable> playbackOn, final BiConsumer<String, Object> sendEvent) {
        _playbackOn = playbackOn;
        _sendEvent = sendEvent;
        tryPlayWelcome();
    }

    private void tryPlayWelcome() {
        try {
            _lock.lock();
            if (_playbackOn != null && _welcome.get() != null) {
                final AIReplyVO welcome = _welcome.getAndSet(null);
                doPlayback(welcome);
            }
        } finally {
            _lock.unlock();
        }
    }

    @Override
    public SpeechTranscriber onSpeechTranscriberCreated(final SpeechTranscriber speechTranscriber) {
        speechTranscriber.setSampleRate(SampleRateEnum.SAMPLE_RATE_16K);
        speechTranscriber.addCustomedParam("speech_noise_threshold", 0.9);

        return super.onSpeechTranscriberCreated(speechTranscriber);
    }

    private boolean isAiSpeaking() {
        return _currentPlaybackId.get() != null;
    }

    public void checkIdle() {
        final long idleTime = System.currentTimeMillis() - _idleStartInMs.get();
        boolean isAiSpeaking = isAiSpeaking();
        if ( _isUserAnswered.get()  // user answered
            && _playbackOn != null  // playback ws has connected
            && !_isUserSpeak.get()  // user not speak
            && !isAiSpeaking        // AI not speak
            && _aiSetting != null
            ) {
            if (idleTime > _aiSetting.getIdle_timeout()) {
                log.info("[{}]-[{}]: checkIdle: idle duration: {} ms >=: [{}] ms\n", _sessionId, _uuid, idleTime, _aiSetting.getIdle_timeout());
                try {
                    final ApiResponse<AIReplyVO> response =
                            _scriptApi.ai_reply(_sessionId, null, idleTime, 0, null, 0);
                    if (response.getData() != null) {
                        if (response.getData().getAi_content_id() != null && doPlayback(response.getData())) {
                            return;
                        }
                    } else {
                        log.info("[{}]-[{}]: checkIdle: ai_reply {}, do nothing\n", _sessionId, _uuid, response);
                    }
                } catch (Exception ex) {
                    log.warn("[{}]-[{}]: checkIdle: ai_reply error, detail: {}", _sessionId, _uuid, ex.toString());
                }
            }
        }
        log.info("[{}]-[{}]: checkIdle: is_speaking: {}/is_playing: {}/idle duration: {} ms",
                _sessionId, _uuid, _isUserSpeak.get(), isAiSpeaking, idleTime);
        /*
        if (!_isUserSpeak.get() && isAiSpeaking() && _currentPlaybackPaused.get() && idleTime >= 2000) {
            // user not speaking & ai speaking and paused and user not speak more than 2s
            if (_currentPlaybackPaused.compareAndSet(true, false)) {
                _sendEvent.accept("PCMResumePlayback", new PayloadPCMEvent(_currentPlaybackId.get(), ""));
                log.info("[{}]: checkIdle: resume current {}", _sessionId, _currentPlaybackId.get());
            }
        }
        */
    }

    @Override
    public boolean transmit(final ByteBuffer bytes) {
        try {
            final boolean result = super.transmit(bytes);

            if (result) {
                final byte[] srcBytes = new byte[bytes.remaining()];
                bytes.get(srcBytes, 0, srcBytes.length);
                if (_asrStartInMs.compareAndSet(0, 1)) {
                    _asrStartInMs.set(System.currentTimeMillis());
                }
                _usBufs.add(srcBytes);
            }

            return result;
        } catch (Exception ex) {
            log.warn("[{}]-[{}]: transmit_asr_failed, detail: {}", _sessionId, _uuid, ex.toString());
            return false;
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
            log.warn("[{}]-[{}]: notifyPlaybackSendStart: current PlaybackSegment is !NOT! null", _sessionId, _uuid);
        } else {
            log.info("[{}]-[{}]: notifyPlaybackSendStart: add new PlaybackSegment", _sessionId, _uuid);
        }
    }

    public void notifyPlaybackSendStop(final String playbackId, final long stopTimestamp) {
        final PlaybackSegment ps = _currentPS.getAndSet(null);
        if (ps != null) {
            _dsBufs.add(ps);
            log.info("[{}]-[{}]: notifyPlaybackSendStop: move current PlaybackSegment to _dsBufs", _sessionId, _uuid);
        } else {
            log.warn("[{}]-[{}]: notifyPlaybackSendStop: current PlaybackSegment is null", _sessionId, _uuid);
        }
    }

    public void notifyPlaybackSendData(final byte[] bytes) {
        final PlaybackSegment ps = _currentPS.get();
        if (ps != null) {
            ps._data.add(bytes);
        } else {
            log.warn("[{}]-[{}]: notifyPlaybackSendData: current PlaybackSegment is null", _sessionId, _uuid);
        }
    }

    @Override
    public void notifySentenceBegin(final PayloadSentenceBegin payload) {
        super.notifySentenceBegin(payload);
        log.info("[{}]-[{}]: notifySentenceBegin: {}", _sessionId, _uuid, payload);
        _isUserSpeak.set(true);
        _currentSentenceBeginInMs.set(System.currentTimeMillis());
        if (isAICancelOnSpeak()) {
            final Runnable stopPlayback = _currentStopPlayback.getAndSet(null);
            if (stopPlayback != null) {
                stopPlayback.run();
            }
            _sendEvent.accept("PCMStopPlayback", new PayloadPCMEvent(_currentPlaybackId.get(), ""));
            log.info("[{}]-[{}]: stop current playing ({}) for cancel_on_speak is true", _sessionId, _uuid, _currentPlaybackId.get());
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
        if (isAiSpeaking()) {
            final int length = payload.getResult().length();
            if (length >= 10) {
                if ( (length - countChinesePunctuations(payload.getResult())) >= 10  && _currentPlaybackPaused.compareAndSet(false, true)) {
                    _sendEvent.accept("PCMPausePlayback", new PayloadPCMEvent(_currentPlaybackId.get(), ""));
                    log.info("[{}]-[{}]: notifyTranscriptionResultChanged: pause_current ({}) for result {} text >= 10",
                            _sessionId, _uuid, _currentPlaybackId.get(), payload.getResult());
                }
            }
        }
    }

    private static int countChinesePunctuations(final String text) {
        int count = 0;
        for (char c : text.toCharArray()) {
            // 判断是否是中文标点
            if (isChinesePunctuation(c)) {
                count++;
            }
        }
        return count;
    }

    // 将需要计算的中文标点放入此数组中
    private static final Character[] punctuations = new Character[] {
            '，', '。', '！', '？', '；', '：', '“', '”'
    };

    private static boolean isChinesePunctuation(final char ch) {
        return Arrays.stream(punctuations).anyMatch(p -> p == ch);
    }

    @Override
    public void notifySentenceEnd(final PayloadSentenceEnd payload) {
        super.notifySentenceEnd(payload);
        log.info("[{}]-[{}]: notifySentenceEnd: {}", _sessionId, _uuid, payload);

        final long sentenceEndInMs = System.currentTimeMillis();
        _isUserSpeak.set(false);
        _idleStartInMs.set(sentenceEndInMs);
        if (_sessionId != null && _playbackOn != null) {
            // _playbackOn != null means playback ws has connected
            final String userContentId = interactWithScriptEngine(payload);
            reportUserContent(payload, userContentId);
            reportAsrTime(payload, sentenceEndInMs, userContentId);
        } else {
            log.warn("[{}]-[{}]: notifySentenceEnd but sessionId is null or playback not ready => _playbackOn {}",
                    _sessionId, _uuid, _playbackOn);
        }
    }

    @Nullable
    private String interactWithScriptEngine(final PayloadSentenceEnd payload) {
        final boolean isAiSpeaking = isAiSpeaking();
        String userContentId = null;
        try {
            final String userSpeechText = payload.getResult();
            final String aiContentId = currentAiContentId();
            final int speakingDuration = currentSpeakingDuration();
            log.info("[{}]-[{}]: ai_reply: speech:{}/is_speaking:{}/content_id:{}/speaking_duration:{} s",
                    _sessionId, _uuid, userSpeechText, isAiSpeaking, aiContentId, (float)speakingDuration / 1000.0f);
            final ApiResponse<AIReplyVO> response =
                    _scriptApi.ai_reply(_sessionId, userSpeechText,null, isAiSpeaking ? 1 : 0, aiContentId, speakingDuration);

            if (response.getData() != null) {
                if (response.getData().getUser_content_id() != null) {
                    userContentId = response.getData().getUser_content_id().toString();
                }
                if (response.getData().getAi_content_id() != null && doPlayback(response.getData())) {
                    // _lastReply = response.getData();
                } else {
                    if (isAiSpeaking && _currentPlaybackPaused.compareAndSet(true, false) ) {
                        _sendEvent.accept("PCMResumePlayback", new PayloadPCMEvent(_currentPlaybackId.get(), ""));
                        log.info("[{}]-[{}]: notifySentenceEnd: resume_current ({}) for ai_reply ({}) without_new_ai_playback",
                                _sessionId, _uuid, _currentPlaybackId.get(), payload.getResult());
                    }
                }
            } else {
                log.info("[{}]-[{}]: notifySentenceEnd: ai_reply {}, do_nothing", _sessionId, _uuid, response);
            }
        } catch (final Exception ex) {
            log.warn("[{}]-[{}]: notifySentenceEnd: ai_reply error, detail: {}", _sessionId, _uuid, ExceptionUtil.exception2detail(ex));
        }
        return userContentId;
    }

    public void notifyPlaybackStart(final String playbackId) {
        log.info("[{}]-[{}]: notifyPlaybackStart => task: {} while currentPlaybackId: {}",
                _sessionId, _uuid, playbackId, _currentPlaybackId.get());
    }

    public void notifyPlaybackStop(final String playbackId,
                                   final String contentId,
                                   final String playback_begin_timestamp,
                                   final String playback_end_timestamp,
                                   final String playback_duration) {
        final String currentPlaybackId = _currentPlaybackId.get();
        if (currentPlaybackId != null) {
            if (currentPlaybackId.equals(playbackId)) {
                // reset playback status
                _currentPlaybackId.set(null);
                _currentStopPlayback.set(null);
                _currentPlaybackPaused.set(false);
                _currentPlaybackDuration.set(()->0L);
                _idleStartInMs.set(System.currentTimeMillis());
                log.info("[{}]-[{}]: notifyPlaybackStop => current playbackid: {} Matched / memo: {}", _sessionId, _uuid, playbackId, memoFor(playbackId));
                if (memoFor(playbackId).isHangup()) {
                    // hangup call
                    _doHangup.accept(this);
                }
            } else {
                log.info("[{}]-[{}]: notifyPlaybackStop => current playbackid: {} mismatch stopped playbackid: {}, ignore",
                        _sessionId, _uuid, currentPlaybackId, playbackId);
            }
        } else {
            log.warn("[{}]-[{}]: currentPlaybackId is null BUT notifyPlaybackStop with: {}", _sessionId, _uuid, playbackId);
        }
        reportAIContent(playbackId, contentId, playback_begin_timestamp, playback_end_timestamp, playback_duration);
    }

    private void reportUserContent(final PayloadSentenceEnd payload, final String userContentId) {
        // report USER speak timing
        // ASR-Sentence-Begin-Time in Milliseconds
        final long start_speak_timestamp = _asrStartInMs.get() + payload.getBegin_time();
        // ASR-Sentence-End-Time in Milliseconds
        final long stop_speak_timestamp = _asrStartInMs.get() + payload.getTime();
        final long user_speak_duration = stop_speak_timestamp - start_speak_timestamp;

        final ApiResponse<Void> resp = _scriptApi.report_content(
                _sessionId,
                userContentId,
                payload.getIndex(),
                "USER",
                _asrStartInMs.get(),
                start_speak_timestamp,
                stop_speak_timestamp,
                user_speak_duration);
        log.info("[{}]-[{}]: user report_content(content_id:{}/idx:{}/asr_start:{}/start_speak:{}/stop_speak:{}/speak_duration:{} s)'s resp: {}",
                _sessionId,
                _uuid,
                userContentId,
                payload.getIndex(),
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

        final ApiResponse<Void> resp = _scriptApi.report_asrtime(
                _sessionId,
                userContentId,
                payload.getIndex(),
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
            log.warn("[{}]-[{}]: notifyPlaybackStop: can't find PlaybackMemo by {}", _sessionId, _uuid, playbackId);
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
        log.info("[{}]-[{}]: ai report_content(content_id:{}/idx:{}/asr_start:{}/start_speak:{}/stop_speak:{}/speak_duration:{} s)'s resp: {}",
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

    public void notifyPlaybackPaused(final String playbackId,
                                     final String contentId,
                                     final String playback_duration) {
        final String currentPlaybackId = _currentPlaybackId.get();
        if (currentPlaybackId != null) {
            if (currentPlaybackId.equals(playbackId)) {
                final long ai_speak_duration = playback_duration != null
                        ? (long)(Float.parseFloat(playback_duration) * 1000L)
                        : 0;
                _currentPlaybackDuration.set(()->ai_speak_duration);
                log.info("[{}]-[{}]: notifyPlaybackPaused => current playbackid: {} Matched / playback_duration: {} ms",
                        _sessionId, _uuid, playbackId, ai_speak_duration);
            } else {
                log.info("[{}]-[{}]: notifyPlaybackPaused => current playbackid: {} mismatch stopped playbackid: {}, ignore",
                        _sessionId, _uuid, currentPlaybackId, playbackId);
            }
        } else {
            log.warn("[{}]-[{}]: currentPlaybackId is null BUT notifyPlaybackPaused with: {}", _sessionId, _uuid, playbackId);
        }
    }

    public void notifyPlaybackResumed(final String playbackId,
                                      final String contentId,
                                      final String playback_duration) {
        final String currentPlaybackId = _currentPlaybackId.get();
        if (currentPlaybackId != null) {
            if (currentPlaybackId.equals(playbackId)) {
                final long ai_speak_duration = playback_duration != null
                        ? (long)(Float.parseFloat(playback_duration) * 1000L)
                        : 0;
                final long playbackResumedInMs = System.currentTimeMillis();
                _currentPlaybackDuration.set(()->ai_speak_duration + (System.currentTimeMillis() - playbackResumedInMs));
                log.info("[{}]-[{}]: notifyPlaybackResumed => current playbackid: {} Matched / playback_duration: {} ms",
                        _sessionId, _uuid, playbackId, ai_speak_duration);
            } else {
                log.info("[{}]-[{}]: notifyPlaybackResumed => current playbackid: {} mismatch stopped playbackid: {}, ignore",
                        _sessionId, _uuid, currentPlaybackId, playbackId);
            }
        } else {
            log.warn("[{}]-[{}]: currentPlaybackId is null BUT notifyPlaybackResumed with: {}", _sessionId, _uuid, playbackId);
        }
    }

    @Override
    public void close() {
        super.close();
        if (_sessionId != null) {
            _callSessions.remove(_sessionId);
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
            log.warn("[{}]: close_without_valid_sessionId", _uuid);
        }
        if (!_isUserAnswered.get()) {
            log.warn("[{}]-[{}]: close_without_user_answer", _sessionId, _uuid);
        }
        if (_playbackOn == null) {
            log.warn("[{}]-[{}]: close_without_playback_ws", _sessionId, _uuid);
        }
    }

    private void generateRecordAndUpload() {
        final String path = _aiSetting.getAnswer_record_file();
        // eg: "answer_record_file": "rms://{uuid={uuid},url=xxxx,bucket=<bucketName>}<objectName>",
        final int braceBegin = path.indexOf('{');
        if (braceBegin == -1) {
            log.warn("[{}]-[{}]: {} missing vars, ignore", _sessionId, _uuid, path);
            return;
        }
        final int braceEnd = path.lastIndexOf('}');
        if (braceEnd == -1) {
            log.warn("[{}]-[{}]: {} missing vars, ignore", _sessionId, _uuid, path);
            return;
        }
        final String vars = path.substring(braceBegin + 1, braceEnd);

        final String bucketName = VarsUtil.extractValue(vars, "bucket");
        if (null == bucketName) {
            log.warn("[{}]-[{}]: {} missing bucket field, ignore", _sessionId, _uuid, path);
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
                    log.info("[{}]-[{}]: current ps overlapped with next ps at timestamp: {}, using_next_ps_as_current", _sessionId, _uuid, next_ps.get().timestamp);
                    fetchPS(ps, next_ps, currentInMs);
                    downsample_is = null;
                }
                if (downsample_is == null && ps.get() != null && ps.get().timestamp == currentInMs) {
                    log.info("[{}]-[{}]: current ps {} match currentInMS", _sessionId, _uuid, ps.get().timestamp);
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

            log.info("[{}]-[{}] generateRecordAndUpload: total written {} samples, {} seconds", _sessionId, _uuid, sample_count, (float)sample_count / 16000);

            bos.flush();
            _doSaveRecord.accept(new RecordContext(_sessionId, bucketName, objectName, new ByteArrayInputStream(bos.toByteArray())));
        } catch (IOException ex) {
            log.warn("[{}]-[{}] generateRecordAndUpload: generate record stream error, detail: {}", _sessionId, _uuid, ex.toString());
            throw new RuntimeException(ex);
        }
    }

    private void fetchPS(final AtomicReference<PlaybackSegment> ps, final AtomicReference<PlaybackSegment> next_ps, final long currentInMs) {
        if (!_dsBufs.isEmpty()) {
            ps.set(_dsBufs.remove(0));
            if (ps.get() != null) {
                log.info("[{}]-[{}]: new current ps's start tm: {} / currentInMS: {}", _sessionId, _uuid, ps.get().timestamp, currentInMs);
            }
            if (!_dsBufs.isEmpty()) {
                // prefetch next ps
                next_ps.set(_dsBufs.get(0));
                if (next_ps.get() != null) {
                    log.info("[{}]-[{}]: next current ps's start tm: {} / currentInMS: {}", _sessionId, _uuid, next_ps.get().timestamp, currentInMs);
                }
            } else {
                next_ps.set(null);
            }
        }
    }

    private boolean doPlayback(final AIReplyVO replyVO) {
        final String newPlaybackId = UUID.randomUUID().toString();
        log.info("[{}]-[{}]: doPlayback: {}", _sessionId, _uuid, replyVO);
        final Supplier<Runnable> playback = reply2playback(newPlaybackId, replyVO);
        if (playback == null) {
            log.info("[{}]-[{}]: doPlayback: unknown reply: {}, ignore", _sessionId, _uuid, replyVO);
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
                        .build());
    }

    private PlaybackMemo memoFor(final String playbackId) {
        return _id2memo.get(playbackId);
    }

    private Supplier<Runnable> reply2playback(final String playbackId, final AIReplyVO replyVO) {
        final String aiContentId = replyVO.getAi_content_id() != null ? Long.toString(replyVO.getAi_content_id()) : null;
        if ("cp".equals(replyVO.getVoiceMode())) {
            return ()->_playbackOn.apply(new PlaybackContext(
                    playbackId,
                    String.format("type=cp,%s", JSON.toJSONString(replyVO.getCps())),
                    aiContentId));
        } else if ("wav".equals(replyVO.getVoiceMode())) {
            return ()->_playbackOn.apply(new PlaybackContext(
                    playbackId,
                    String.format("{bucket=%s}%s%s", _bucket, _wavPath, replyVO.getAi_speech_file()),
                    aiContentId));
        } else if ("tts".equals(replyVO.getVoiceMode())) {
            return ()->_playbackOn.apply(new PlaybackContext(
                    playbackId,
                    String.format("{type=tts,text=%s}tts.wav",
                            StringUnicodeEncoderDecoder.encodeStringToUnicodeSequence(replyVO.getReply_content())),
                    aiContentId));
        } else {
            return null;
        }
    }

    static public PoActor findBy(final String sessionId) {
        return _callSessions.get(sessionId);
    }

    private BiConsumer<String, Object> _sendEvent;

    private final AtomicBoolean _isUserAnswered = new AtomicBoolean(false);

    private final String _uuid;
    private final String _tid;
    private final CallApi _callApi;
    private final ScriptApi _scriptApi;
    private final Consumer<PoActor> _doHangup;
    private final String _bucket;
    private final String _wavPath;
    private final AtomicReference<AIReplyVO> _welcome = new AtomicReference<>(null);
    private AiSettingVO _aiSetting;

    private Function<PlaybackContext, Runnable> _playbackOn;
    private final AtomicReference<Runnable> _currentStopPlayback = new AtomicReference<>(null);
    private final AtomicReference<String> _currentPlaybackId = new AtomicReference<>(null);
    private final AtomicBoolean _currentPlaybackPaused = new AtomicBoolean(false);
    private final AtomicReference<Supplier<Long>> _currentPlaybackDuration = new AtomicReference<>(()->0L);

    private final AtomicLong _idleStartInMs = new AtomicLong(System.currentTimeMillis());
    private final AtomicBoolean _isUserSpeak = new AtomicBoolean(false);
    private final AtomicLong _currentSentenceBeginInMs = new AtomicLong(-1);

    private final AtomicInteger _playbackIdx = new AtomicInteger(0);

    private final ConcurrentMap<String, PlaybackMemo> _id2memo = new ConcurrentHashMap<>();

    private static final ConcurrentMap<String, PoActor> _callSessions = new ConcurrentHashMap<>();

    private final List<byte[]> _usBufs = new ArrayList<>();

    @RequiredArgsConstructor
    private static class PlaybackSegment {
        final long timestamp;
        final List<byte[]> _data = new LinkedList<>();
    }
    private final AtomicReference<PlaybackSegment> _currentPS = new AtomicReference<>(null);
    private final List<PlaybackSegment> _dsBufs = new ArrayList<>();

    private final AtomicLong _asrStartInMs = new AtomicLong(0);
    private final Consumer<RecordContext> _doSaveRecord;
}
