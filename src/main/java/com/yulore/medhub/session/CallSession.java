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

    public CallSession(final CallApi callApi, final ScriptApi scriptApi, final Runnable doHangup, final String bucket, final String wavPath, final Consumer<RecordContext> doRecord) {
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
        try {
            final String kid = cmd.getPayload() != null ? cmd.getPayload().get("kid") : "";
            final String tid = cmd.getPayload() != null ? cmd.getPayload().get("tid") : "";
            final String realName = cmd.getPayload() != null ? cmd.getPayload().get("realName") : "";
            final String genderStr = cmd.getPayload() != null ? cmd.getPayload().get("genderStr") : "";

            final ApiResponse<ApplySessionVO> response = _callApi.apply_session(kid, tid, realName, null, genderStr, null, System.currentTimeMillis());
            _sessionId = response.getData().getSessionId();
            log.info("[{}]: userAnswer: kid:{}/tid:{}/realName:{}/gender:{} => response: {}", _sessionId, kid, tid, realName, genderStr, response);

            _lastReply = response.getData();
            _aiSetting = response.getData().getAiSetting();
            _callSessions.put(_sessionId, this);
            log.info("apply session response: {}", response);
            HubEventVO.sendEvent(webSocket, "CallStarted", new PayloadCallStarted(response.getData().getSessionId()));
        } catch (Exception ex) {
            log.warn("failed for _scriptApi.apply_session, detail: {}", ex.toString());
        }
    }

    public void checkIdle() {
        final long idleTime = System.currentTimeMillis() - Math.max(_idleStartInMs.get(), _playback != null ? _playback.idleStartInMs() : 0);
        boolean isAiSpeaking = _playback != null && _playback.isPlaying();
        if (_sessionId != null      // user answered
            && !_isUserSpeak.get()  // user not speak
            && !isAiSpeaking        // AI not speak
            ) {
            if (idleTime > CHECK_IDLE_TIMEOUT) {
                log.info("checkIdle: idle duration: {} ms >=: [{}] ms\n", idleTime, CHECK_IDLE_TIMEOUT);
                try {
                    final ApiResponse<AIReplyVO> response =
                            _scriptApi.ai_reply(_sessionId, null, 0, idleTime);
                    if (response.getData() != null) {
                        if (doPlayback(response.getData())) {
                            _lastReply = response.getData();
                        }
                    } else {
                        log.info("checkIdle: ai_reply {}, do nothing\n", response);
                    }
                } catch (Exception ex) {
                    log.warn("checkIdle: ai_reply error, detail: {}", ex.toString());
                }
            }
        }
        log.info("checkIdle: sessionId: {}/is_speaking: {}/is_playing: {}/idle duration: {} ms",
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
        }
    }

    public void notifyPlaybackSendStop(final long stopTimestamp) {
        final PlaybackSegment ps = _currentPS.getAndSet(null);
        if (ps != null) {
            _dsBufs.add(ps);
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
        if (_lastReply != null && _lastReply.getPause_on_speak() != null && _lastReply.getPause_on_speak()) {
            // log.info("notifySentenceBegin: lastReply: {}, pauseCurrentAnyway", _lastReply);
            _playback.pauseCurrent();
        } else {
            // log.info("notifySentenceBegin: lastReply: {}, ignore", _lastReply);
        }
    }

    @Override
    public void notifySentenceEnd(final PayloadSentenceEnd payload) {
        super.notifySentenceEnd(payload);
        _isUserSpeak.set(false);
        _idleStartInMs.set(System.currentTimeMillis());

        if (_lastReply != null && _lastReply.getPause_on_speak() != null && _lastReply.getPause_on_speak()) {
            // log.info("notifySentenceEnd: lastReply: {}, resumeCurrentAnyway", _lastReply);
            _playback.resumeCurrent();
        } else {
            // log.info("notifySentenceEnd: lastReply: {}, ignore", _lastReply);
        }

        if (_sessionId != null) {
            boolean isAiSpeaking = _playback != null && _playback.isPlaying();
            try {
                final ApiResponse<AIReplyVO> response =
                        _scriptApi.ai_reply(_sessionId, payload.getResult(), isAiSpeaking ? 1 : 0, null);
                if (response.getData() != null) {
                    if (doPlayback(response.getData())) {
                        _lastReply = response.getData();
                    }
                } else {
                    log.info("notifySentenceEnd: ai_reply {}, do nothing\n", response);
                }
            } catch (Exception ex) {
                log.warn("notifySentenceEnd: ai_reply error, detail: {}", ex.toString());
            }
        }
    }

    public void notifyPlaybackStart(final PlayStreamPCMTask task) {
    }

    public void notifyPlaybackStop(final PlayStreamPCMTask task) {
        if (_lastReply != null && _lastReply.getHangup() == 1) {
            // hangup call
            _doHangup.run();
        }
    }

    @Override
    public void close() {
        super.close();
        _callSessions.remove(_sessionId);
        if (_playback != null) {
            // stop current playback when call close()
            _playback.stopCurrent();
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
        final int braceEnd = path.indexOf('}');
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
            PlaybackSegment ps = null;

            if (!_dsBufs.isEmpty()) {
                ps = _dsBufs.remove(0);
                if (ps != null) {
                    log.info("new current ps's start tm: {} / currentInMS: {}", ps.timestamp, currentInMs);
                }
            }

            int sample_count = 0;
            while (upsample_is.read(one_sample) == 2) {
                if (downsample_is == null && ps != null && ps.timestamp == currentInMs) {
                    log.info("current ps {} match currentInMS", ps.timestamp);
                    downsample_is = new ByteArrayListInputStream(ps._data);
                }
                sample_count++;
                boolean downsample_written = false;
                // read up stream data ok
                bos.write(one_sample);
                if (downsample_is != null) {
                    if (downsample_is.read(one_sample) == 2) {
                        bos.write(one_sample);
                        downsample_written = true;
                    } else {
                        // current ps written
                        downsample_is.close();
                        downsample_is = null;
                        if (!_dsBufs.isEmpty()) {
                            ps = _dsBufs.remove(0);
                            if (ps != null) {
                                log.info("new current ps's start tm: {} / currentInMS: {}", ps.timestamp, currentInMs);
                            }
                        }
                    }
                }
                if (!downsample_written) {
                    // silent data
                    one_sample[0] = (byte)0x00;
                    one_sample[1] = (byte)0x00;
                    bos.write(one_sample);
                }
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

    public void attach(final PlaybackSession playback, final Consumer<String> playbackOn) {
        _playback = playback;
        _playbackOn = playbackOn;
        doPlayback(_lastReply);
    }

    private boolean doPlayback(final AIReplyVO replyVO) {
        log.info("doPlayback: {}", replyVO);
        if ("cp".equals(replyVO.getVoiceMode())) {
            _playbackOn.accept(String.format("type=cp,%s", JSON.toJSONString(replyVO.getCps())));
        } else if ("wav".equals(replyVO.getVoiceMode())) {
            _playbackOn.accept(String.format("{bucket=%s}%s%s", _bucket, _wavPath, replyVO.getAi_speech_file()));
        } else if ("tts".equals(replyVO.getVoiceMode())) {
            _playbackOn.accept(String.format("{type=tts,text=%s}tts.wav",
                    StringUnicodeEncoderDecoder.encodeStringToUnicodeSequence(replyVO.getReply_content())));
        } else {
            log.info("doPlayback: unknown reply: {}, ignore", replyVO);
            return false;
        }
        return true;
    }

    static public CallSession findBy(final String sessionId) {
        return _callSessions.get(sessionId);
    }

    private final CallApi _callApi;
    private final ScriptApi _scriptApi;
    private final Runnable _doHangup;
    private final String _bucket;
    private final String _wavPath;
    private AIReplyVO _lastReply;
    private AiSettingVO _aiSetting;

    private Consumer<String> _playbackOn;
    private PlaybackSession _playback;
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
