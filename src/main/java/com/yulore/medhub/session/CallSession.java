package com.yulore.medhub.session;

import com.alibaba.fastjson.JSON;
import com.alibaba.nls.client.protocol.SampleRateEnum;
import com.alibaba.nls.client.protocol.asr.SpeechTranscriber;
import com.mgnt.utils.StringUnicodeEncoderDecoder;
import com.yulore.medhub.api.*;
import com.yulore.medhub.stream.VarsUtil;
import com.yulore.medhub.task.PlayStreamPCMTask;
import com.yulore.medhub.vo.*;
import com.yulore.util.ByteArrayListInputStream;
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
import java.util.function.Consumer;

@ToString
@Slf4j
public class CallSession extends ASRSession {

    @AllArgsConstructor
    static public class RecordContext {
        public String sessionId;
        public String bucketName;
        public String objectName;
        public InputStream content;
    }

    static final long CHECK_IDLE_TIMEOUT = 5000L; // 5 seconds to report check idle to script engine

    public CallSession(final CallApi callApi,
                       final ScriptApi scriptApi,
                       final Consumer<CallSession> doHangup,
                       final String bucket,
                       final String wavPath,
                       final Consumer<RecordContext> doRecord) {
        _sessionId = null;
        _scriptApi = scriptApi;
        _callApi = callApi;
        _doHangup = doHangup;
        _bucket = bucket;
        _wavPath = wavPath;
        _doRecord = doRecord;
    }

    @Override
    public SpeechTranscriber onSpeechTranscriberCreated(final SpeechTranscriber speechTranscriber) {
        speechTranscriber.setSampleRate(SampleRateEnum.SAMPLE_RATE_16K);

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

    public void checkIdle() {
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
                            _scriptApi.ai_reply(_sessionId, null, 0, idleTime);
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
        if (_recordStartTimestamp.compareAndSet(0, 1)) {
            _recordStartTimestamp.set(System.currentTimeMillis());
        }
        _usBufs.add(srcBytes);

        return result;
    }

    public void notifyPlaybackSendStart(final long startTimestamp) {
        if (!_currentPS.compareAndSet(null, new PlaybackSegment(startTimestamp))) {
            log.warn("[{}]: notifyPlaybackSendStart: current PlaybackSegment is !NOT! null", _sessionId);
        } else {
            log.info("[{}]: notifyPlaybackSendStart: add new PlaybackSegment", _sessionId);
        }
    }

    public void notifyPlaybackSendStop(final long stopTimestamp) {
        final PlaybackSegment ps = _currentPS.getAndSet(null);
        if (ps != null) {
            _dsBufs.add(ps);
            log.info("[{}]: notifyPlaybackSendStop: move current PlaybackSegment to _dsBufs", _sessionId);
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
        _isUserSpeak.set(true);
        if (null != _sessionId) {
            log.info("[{}]: notifySentenceBegin: {}", _sessionId, payload);
        }
        if (_playback.get() != null && _lastReply != null && _lastReply.getPause_on_speak() != null && _lastReply.getPause_on_speak()) {
            _playback.get().pauseCurrent();
            log.info("[{}]: pauseCurrent: {}", _sessionId, _playback.get());
        }
    }

    @Override
    public void notifySentenceEnd(final PayloadSentenceEnd payload) {
        super.notifySentenceEnd(payload);
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
                        _scriptApi.ai_reply(_sessionId, payload.getResult(), isAiSpeaking ? 1 : 0, null);
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
    }

    public void notifyPlaybackStart(final PlayStreamPCMTask task) {
        log.info("[{}]: notifyPlaybackStart => task: {}", _sessionId, task);
    }

    public void notifyPlaybackStop(final PlayStreamPCMTask task) {
        log.info("[{}]: notifyPlaybackStop => task: {} / lastReply: {}", _sessionId, task, _lastReply);
        if (_lastReply != null && _lastReply.getHangup() == 1) {
            // hangup call
            _doHangup.accept(this);
        }
    }

    @Override
    public void close() {
        super.close();
        _callSessions.remove(_sessionId);
        if (_playback.get() != null) {
            // stop current playback when call close()
            _playback.get().stopCurrent();
        }
        generateRecordAndUpload();
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

            long currentInMs = _recordStartTimestamp.get();
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
                    log.warn("current ps overlapped with next ps at timestamp: {}, using_next_ps_as_current", next_ps.get().timestamp);
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

            log.info("total written {} samples, {} seconds", sample_count, (float)sample_count / 16000);

            bos.flush();
            _doRecord.accept(new RecordContext(_sessionId, bucketName, objectName, new ByteArrayInputStream(bos.toByteArray())));
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

    public void attach(final PlaybackSession playback, final Consumer<String> playbackOn) {
        _playback.set(playback);
        _playbackOn = playbackOn;
        doPlayback(_lastReply);
    }

    private boolean doPlayback(final AIReplyVO replyVO) {
        log.info("[{}]: doPlayback: {}", _sessionId, replyVO);
        if ("cp".equals(replyVO.getVoiceMode())) {
            _playbackOn.accept(String.format("type=cp,%s", JSON.toJSONString(replyVO.getCps())));
        } else if ("wav".equals(replyVO.getVoiceMode())) {
            _playbackOn.accept(String.format("{bucket=%s}%s%s", _bucket, _wavPath, replyVO.getAi_speech_file()));
        } else if ("tts".equals(replyVO.getVoiceMode())) {
            _playbackOn.accept(String.format("{type=tts,text=%s}tts.wav",
                    StringUnicodeEncoderDecoder.encodeStringToUnicodeSequence(replyVO.getReply_content())));
        } else {
            log.info("[{}]: doPlayback: unknown reply: {}, ignore", _sessionId, replyVO);
            return false;
        }
        return true;
    }

    static public CallSession findBy(final String sessionId) {
        return _callSessions.get(sessionId);
    }

    private final AtomicBoolean _isUserAnswered = new AtomicBoolean(false);

    private final CallApi _callApi;
    private final ScriptApi _scriptApi;
    private final Consumer<CallSession> _doHangup;
    private final String _bucket;
    private final String _wavPath;
    private AIReplyVO _lastReply;
    private AiSettingVO _aiSetting;

    private Consumer<String> _playbackOn;
    private final AtomicReference<PlaybackSession> _playback = new AtomicReference<>(null);
    private final AtomicLong _idleStartInMs = new AtomicLong(System.currentTimeMillis());
    private final AtomicBoolean _isUserSpeak = new AtomicBoolean(false);

    private static final ConcurrentMap<String, CallSession> _callSessions = new ConcurrentHashMap<>();

    private final List<byte[]> _usBufs = new ArrayList<>();

    @RequiredArgsConstructor
    private static class PlaybackSegment {
        final long timestamp;
        final List<byte[]> _data = new LinkedList<>();
    }
    private final AtomicReference<PlaybackSegment> _currentPS = new AtomicReference<>(null);
    private final List<PlaybackSegment> _dsBufs = new ArrayList<>();

    private final AtomicLong _recordStartTimestamp = new AtomicLong(0);
    private final Consumer<RecordContext> _doRecord;
}
