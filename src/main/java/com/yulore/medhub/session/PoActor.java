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
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Phone Operator Actor
 */
@ToString
@Slf4j
public class PoActor extends ASRActor {

    @AllArgsConstructor
    static public class RecordContext {
        public String sessionId;
        public String bucketName;
        public String objectName;
        public InputStream content;
    }

    static final long CHECK_IDLE_TIMEOUT = 5000L; // 5 seconds to report check idle to script engine

    public PoActor(final CallApi callApi,
                   final ScriptApi scriptApi,
                   final Consumer<PoActor> doHangup,
                   final String bucket,
                   final String wavPath,
                   final Consumer<RecordContext> saveRecord) {
        _sessionId = null;
        _scriptApi = scriptApi;
        _callApi = callApi;
        _doHangup = doHangup;
        _bucket = bucket;
        _wavPath = wavPath;
        _doSaveRecord = saveRecord;
    }

    @Override
    public SpeechTranscriber onSpeechTranscriberCreated(final SpeechTranscriber speechTranscriber) {
        speechTranscriber.setSampleRate(SampleRateEnum.SAMPLE_RATE_16K);
        speechTranscriber.addCustomedParam("speech_noise_threshold", 0.9);

        return super.onSpeechTranscriberCreated(speechTranscriber);
    }

    public void notifyUserAnswer(final HubCommandVO cmd, final WebSocket webSocket) {
        if (!_isUserAnswered.compareAndSet(false, true)) {
            log.warn("[{}]: notifyUserAnswer called already, ignore!", _sessionId);
            return;
        }
        try {
            final String kid = cmd.getPayload() != null ? cmd.getPayload().get("kid") : "";
            final String tid = cmd.getPayload() != null ? cmd.getPayload().get("tid") : "";
            final String realName = cmd.getPayload() != null ? cmd.getPayload().get("realName") : "";
            final String aesMobile = cmd.getPayload() != null ? cmd.getPayload().get("aesMobile") : "";
            final String genderStr = cmd.getPayload() != null ? cmd.getPayload().get("genderStr") : "";

            final ApiResponse<ApplySessionVO> response = _callApi.apply_session(CallApi.ApplySessionRequest.builder()
                    .kid(kid)
                    .tid(tid)
                    .realName(realName)
                    .genderStr(genderStr)
                    .aesMobile(aesMobile)
                    .answerTime(System.currentTimeMillis())
                    .build());
            log.info("apply_session => response: {}", response);
            _sessionId = response.getData().getSessionId();
            log.info("[{}]: userAnswer: kid:{}/tid:{}/realName:{}/gender:{}/aesMobile:{} => response: {}", _sessionId, kid, tid, realName, genderStr, aesMobile, response);

            _lastReply = response.getData();
            _aiSetting = response.getData().getAiSetting();
            _callSessions.put(_sessionId, this);
            HubEventVO.sendEvent(webSocket, "CallStarted", new PayloadCallStarted(response.getData().getSessionId()));
        } catch (Exception ex) {
            log.warn("failed for callApi.apply_session, detail: {}", ex.toString());
        }
    }

    private boolean isAiSpeaking() {
        return _currentPlaybackId.get() != null;
    }

    public void checkIdle() {
        final long idleTime = System.currentTimeMillis() - _idleStartInMs.get();
        boolean isAiSpeaking = isAiSpeaking();
        if (_sessionId != null      // user answered
            && !_isUserSpeak.get()  // user not speak
            && !isAiSpeaking        // AI not speak
            && _aiSetting != null
            ) {
            if (idleTime > _aiSetting.getIdle_timeout()) {
                log.info("[{}]: checkIdle: idle duration: {} ms >=: [{}] ms\n", _sessionId, idleTime, CHECK_IDLE_TIMEOUT);
                try {
                    final ApiResponse<AIReplyVO> response =
                            _scriptApi.ai_reply(_sessionId, null, idleTime, 0, null, 0);
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
    }

    @Override
    public boolean transmit(final ByteBuffer bytes) {
        final boolean result = super.transmit(bytes);

        final byte[] srcBytes = new byte[bytes.remaining()];
        bytes.get(srcBytes, 0, srcBytes.length);
        if (_recordStartInMs.compareAndSet(0, 1)) {
            _recordStartInMs.set(System.currentTimeMillis());
        }
        _usBufs.add(srcBytes);

        return result;
    }

    private String currentAiContentId() {
        return isAiSpeaking() ? (_lastReply != null && _lastReply.getAi_content_id() != null ? Long.toString(_lastReply.getAi_content_id()) : null) : null;
    }

    private int currentSpeakingDuration() {
        return isAiSpeaking() ? (int) (System.currentTimeMillis() - _currentPlaybackBeginInMs.get()) : 0;
    }

    public void notifyPlaybackSendStart(final String contentId, final long startTimestamp) {
        _currentPlaybackBeginInMs.set(startTimestamp);
        if (!_currentPS.compareAndSet(null, new PlaybackSegment(startTimestamp))) {
            log.warn("[{}]: notifyPlaybackSendStart: current PlaybackSegment is !NOT! null", _sessionId);
        } else {
            log.info("[{}]: notifyPlaybackSendStart: add new PlaybackSegment", _sessionId);
        }
    }

    public void notifyPlaybackSendStop(final String contentId, final long stopTimestamp) {
        final PlaybackSegment ps = _currentPS.getAndSet(null);
        if (ps != null) {
            _dsBufs.add(ps);
            log.info("[{}]: notifyPlaybackSendStop: move current PlaybackSegment to _dsBufs", _sessionId);
            {
                // report AI speak timing
                final long start_speak_timestamp = ps.timestamp;
                final long user_speak_duration = stopTimestamp - start_speak_timestamp;

                final ApiResponse<Void> resp = _scriptApi.report_content(
                        _sessionId,
                        contentId,
                        _dsBufs.size(),
                        "AI",
                        _recordStartInMs.get(),
                        start_speak_timestamp,
                        stopTimestamp,
                        user_speak_duration);
                log.info("[{}]: ai report_content({})'s resp: {}", _sessionId, contentId, resp);
            }
        } else {
            log.warn("[{}]: notifyPlaybackSendStop: current PlaybackSegment is null", _sessionId);
        }
    }

    public void notifyPlaybackSendData(final byte[] bytes) {
        final PlaybackSegment ps = _currentPS.get();
        if (ps != null) {
            ps._data.add(bytes);
        } else {
            log.warn("[{}]: notifyPlaybackSendData: current PlaybackSegment is null", _sessionId);
        }
    }

    @Override
    public void notifySentenceBegin(final PayloadSentenceBegin payload) {
        super.notifySentenceBegin(payload);
        log.info("[{}]: notifySentenceBegin: {}", _sessionId, payload);
        _isUserSpeak.set(true);
        _currentSentenceBeginInMs.set(System.currentTimeMillis());
        if (isAiSpeaking() && _lastReply != null && _lastReply.getCancel_on_speak() != null && _lastReply.getCancel_on_speak()) {
            _sendEvent.accept("PCMStopPlayback", new PayloadPCMEvent(_currentPlaybackId.get(), ""));
            log.info("[{}]: stop current playing ({}) for cancel_on_speak: {}", _sessionId, _currentPlaybackId.get(), _lastReply);
        }
    }

    @Override
    public void notifyTranscriptionResultChanged(final PayloadTranscriptionResultChanged payload) {
        super.notifyTranscriptionResultChanged(payload);
        if (isAiSpeaking()) {
            if (payload.getResult().length() >= 3) {
                _sendEvent.accept("PCMPausePlayback", new PayloadPCMEvent(_currentPlaybackId.get(), ""));
                log.info("[{}]: notifyTranscriptionResultChanged: pause current for result {} text >= 3", _sessionId, payload.getResult());
            }
        }
    }

    @Override
    public void notifySentenceEnd(final PayloadSentenceEnd payload) {
        super.notifySentenceEnd(payload);
        log.info("[{}]: notifySentenceEnd: {}", _sessionId, payload);

        final long sentenceEndInMs = System.currentTimeMillis();
        _isUserSpeak.set(false);
        _idleStartInMs.set(sentenceEndInMs);
        if (_sessionId != null && _playbackOn != null) {
            // _playbackOn != null means playback ws has connected
            final boolean isAiSpeaking = isAiSpeaking();;
            String userContentId = null;
            try {
                final ApiResponse<AIReplyVO> response =
                        _scriptApi.ai_reply(_sessionId, payload.getResult(),null, isAiSpeaking ? 1 : 0, currentAiContentId(), currentSpeakingDuration());
                if (response.getData() != null) {
                    if (response.getData().getUser_content_id() != null) {
                        userContentId = response.getData().getUser_content_id().toString();
                    }
                    if (response.getData().getAi_content_id() != null && doPlayback(response.getData())) {
                        _lastReply = response.getData();
                    } else {
                        if (isAiSpeaking) {
                            _sendEvent.accept("PCMResumePlayback", new PayloadPCMEvent(_currentPlaybackId.get(), ""));
                            log.info("[{}]: notifySentenceEnd: resume current for ai_reply {} do nothing", _sessionId, payload.getResult());
                        }
                    }
                } else {
                    log.info("[{}]: notifySentenceEnd: ai_reply {}, do nothing\n", _sessionId, response);
                }
            } catch (Exception ex) {
                log.warn("[{}]: notifySentenceEnd: ai_reply error, detail: {}", _sessionId, ExceptionUtil.exception2detail(ex));
            }

            {
                // report USER speak timing
                // ASR-Sentence-Begin-Time in Milliseconds
                final long start_speak_timestamp = _recordStartInMs.get() + payload.getBegin_time();
                // ASR-Sentence-End-Time in Milliseconds
                final long stop_speak_timestamp = _recordStartInMs.get() + payload.getTime();
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
                log.info("[{}]: user report_content({})'s resp: {}", _sessionId, userContentId, resp);
            }
            {
                // report ASR event timing
                // sentence_begin_event_time in Milliseconds
                final long begin_event_time = _currentSentenceBeginInMs.get() - _recordStartInMs.get();

                // sentence_end_event_time in Milliseconds
                final long end_event_time = sentenceEndInMs - _recordStartInMs.get();

                final ApiResponse<Void> resp = _scriptApi.report_asrtime(
                        _sessionId,
                        userContentId,
                        payload.getIndex(),
                        begin_event_time,
                        end_event_time);
                log.info("[{}]: user report_asrtime({})'s resp: {}", _sessionId, userContentId, resp);
            }
        } else {
            log.warn("[{}]: notifySentenceEnd but sessionId is null or playback not ready => _playbackOn {}", _sessionId, _playbackOn);
        }
    }

    public void notifyPlaybackStart(final String playbackId) {
        log.info("[{}]: notifyPlaybackStart => task: {}", _sessionId, playbackId);
        _currentPlaybackId.set(playbackId);
    }

    public void notifyPlaybackStop(final String playbackId) {
        final String currentPlaybackId = _currentPlaybackId.get();
        if (currentPlaybackId != null) {
            if (currentPlaybackId.equals(playbackId)) {
                _currentPlaybackId.set(null);
                _idleStartInMs.set(System.currentTimeMillis());
                log.info("[{}]: notifyPlaybackStop => current playbackid: {} Matched / lastReply: {}", _sessionId, playbackId, _lastReply);
                if (_lastReply != null && _lastReply.getHangup() == 1) {
                    // hangup call
                    _doHangup.accept(this);
                }
            } else {
                log.info("[{}]: notifyPlaybackStop => current playbackid: {} mismatch stopped playbackid: {}, ignore", _sessionId, currentPlaybackId, playbackId);
            }
        } else {
            log.warn("currentPlaybackId is null BUT notifyPlaybackStop with: {}", playbackId);
        }
    }

    @Override
    public void close() {
        super.close();
        if (_sessionId != null) {
            _callSessions.remove(_sessionId);
            if (isAiSpeaking()) {
                // stop current playback when call close()
                _sendEvent.accept("PCMStopPlayback", new PayloadPCMEvent(_currentPlaybackId.get(), ""));
            }
            generateRecordAndUpload();
        } else {
            log.warn("PoActor: close_without_valid_sessionId");
        }
    }

    private void generateRecordAndUpload() {
        final String path = _aiSetting.getAnswer_record_file();
        // eg: "answer_record_file": "rms://{uuid={uuid},url=xxxx,bucket=<bucketName>}<objectName>",
        final int braceBegin = path.indexOf('{');
        if (braceBegin == -1) {
            log.warn("{} missing vars, ignore", path);
            return;
        }
        final int braceEnd = path.lastIndexOf('}');
        if (braceEnd == -1) {
            log.warn("{} missing vars, ignore", path);
            return;
        }
        final String vars = path.substring(braceBegin + 1, braceEnd);

        final String bucketName = VarsUtil.extractValue(vars, "bucket");
        if (null == bucketName) {
            log.warn("{} missing bucket field, ignore", path);
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

            long currentInMs = _recordStartInMs.get();
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
                    log.info("current ps overlapped with next ps at timestamp: {}, using_next_ps_as_current", next_ps.get().timestamp);
                    fetchPS(ps, next_ps, currentInMs);
                    downsample_is = null;
                }
                if (downsample_is == null && ps.get() != null && ps.get().timestamp == currentInMs) {
                    log.info("current ps {} match currentInMS", ps.get().timestamp);
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

            log.info("[{}]: total written {} samples, {} seconds", _sessionId, sample_count, (float)sample_count / 16000);

            bos.flush();
            _doSaveRecord.accept(new RecordContext(_sessionId, bucketName, objectName, new ByteArrayInputStream(bos.toByteArray())));
        } catch (IOException ex) {
            log.warn("[{}] close: generate record stream error, detail: {}", _sessionId, ex.toString());
            throw new RuntimeException(ex);
        }
    }

    private void fetchPS(final AtomicReference<PlaybackSegment> ps, final AtomicReference<PlaybackSegment> next_ps, final long currentInMs) {
        if (!_dsBufs.isEmpty()) {
            ps.set(_dsBufs.remove(0));
            if (ps.get() != null) {
                log.info("new current ps's start tm: {} / currentInMS: {}", ps.get().timestamp, currentInMs);
            }
            if (!_dsBufs.isEmpty()) {
                // prefetch next ps
                next_ps.set(_dsBufs.get(0));
                if (next_ps.get() != null) {
                    log.info("next current ps's start tm: {} / currentInMS: {}", next_ps.get().timestamp, currentInMs);
                }
            } else {
                next_ps.set(null);
            }
        }
    }

    public void attachPlaybackWs(final BiConsumer<String, String> playbackOn, final BiConsumer<String, Object> sendEvent) {
        _playbackOn = playbackOn;
        doPlayback(_lastReply);
        _sendEvent = sendEvent;
    }

    private boolean doPlayback(final AIReplyVO replyVO) {
        log.info("[{}]: doPlayback: {}", _sessionId, replyVO);
        final String aiContentId = replyVO.getAi_content_id() != null ? Long.toString(replyVO.getAi_content_id()) : null;
        if ("cp".equals(replyVO.getVoiceMode())) {
            _playbackOn.accept(String.format("type=cp,%s", JSON.toJSONString(replyVO.getCps())), aiContentId);
        } else if ("wav".equals(replyVO.getVoiceMode())) {
            _playbackOn.accept(String.format("{bucket=%s}%s%s", _bucket, _wavPath, replyVO.getAi_speech_file()), aiContentId);
        } else if ("tts".equals(replyVO.getVoiceMode())) {
            _playbackOn.accept(String.format("{type=tts,text=%s}tts.wav",
                    StringUnicodeEncoderDecoder.encodeStringToUnicodeSequence(replyVO.getReply_content())), aiContentId);
        } else {
            log.info("[{}]: doPlayback: unknown reply: {}, ignore", _sessionId, replyVO);
            return false;
        }
        return true;
    }

    static public PoActor findBy(final String sessionId) {
        return _callSessions.get(sessionId);
    }

    private BiConsumer<String, Object> _sendEvent;

    private final AtomicBoolean _isUserAnswered = new AtomicBoolean(false);

    private final CallApi _callApi;
    private final ScriptApi _scriptApi;
    private final Consumer<PoActor> _doHangup;
    private final String _bucket;
    private final String _wavPath;
    private AIReplyVO _lastReply;
    private AiSettingVO _aiSetting;

    private BiConsumer<String, String> _playbackOn;
    private final AtomicReference<String> _currentPlaybackId = new AtomicReference<>(null);
    private final AtomicLong _currentPlaybackBeginInMs = new AtomicLong(-1);

    private final AtomicLong _idleStartInMs = new AtomicLong(System.currentTimeMillis());
    private final AtomicBoolean _isUserSpeak = new AtomicBoolean(false);
    private final AtomicLong _currentSentenceBeginInMs = new AtomicLong(-1);

    private static final ConcurrentMap<String, PoActor> _callSessions = new ConcurrentHashMap<>();

    private final List<byte[]> _usBufs = new ArrayList<>();

    @RequiredArgsConstructor
    private static class PlaybackSegment {
        final long timestamp;
        final List<byte[]> _data = new LinkedList<>();
    }
    private final AtomicReference<PlaybackSegment> _currentPS = new AtomicReference<>(null);
    private final List<PlaybackSegment> _dsBufs = new ArrayList<>();

    private final AtomicLong _recordStartInMs = new AtomicLong(0);
    private final Consumer<RecordContext> _doSaveRecord;
}
