package com.yulore.medhub.ws.actor;

import com.yulore.medhub.api.AIReplyVO;
import com.yulore.medhub.api.ApiResponse;
import com.yulore.medhub.api.ScriptApi;
import com.yulore.medhub.service.ASRConsumer;
import com.yulore.medhub.service.ASROperator;
import com.yulore.medhub.service.ASRService;
import com.yulore.medhub.vo.*;
import com.yulore.medhub.vo.cmd.AFSAddLocal;
import com.yulore.util.ExceptionUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.springframework.beans.factory.config.ConfigurableBeanFactory.SCOPE_PROTOTYPE;

@Slf4j
@Component
@Scope(SCOPE_PROTOTYPE)
public class AfsActor {
    public AfsActor(final AFSAddLocal payload) {
        localIdx = payload.localIdx;
        uuid = payload.uuid;
        sessionId = payload.sessionId;
        welcome = payload.welcome;
    }

    private final int localIdx;
    private final String uuid;
    private final String sessionId;
    private final String welcome;

    @Autowired
    private ASRService _asrService;

    @Resource
    private ScriptApi _scriptApi;

    private final AtomicReference<ASROperator> opRef = new AtomicReference<>(null);

    @PostConstruct
    private void playWelcome() {
        try {
            final ApiResponse<AIReplyVO> response =
                    _scriptApi.ai_reply(sessionId, welcome, null, 0, null, 0);
            log.warn("[{}]: playWelcome: call ai_reply with {} => response: {}", sessionId, welcome, response);
            if (response.getData() != null) {
                if (doPlayback(response.getData())) {
                } else if (response.getData().getHangup() == 1) {
//                    _sendEvent.accept("FSHangup", new PayloadFSHangup(_uuid, _sessionId));
//                    log.info("[{}]: playWelcome: hangup ({}) for ai_reply ({})", _sessionId, _sessionId, response.getData());
                }
            } else {
//                _sendEvent.accept("FSHangup", new PayloadFSHangup(_uuid, _sessionId));
//                log.warn("[{}]: transcriptionStarted: ai_reply({}), hangup", _sessionId, response);
            }
        } catch (final Exception ex) {
//            _sendEvent.accept("FSHangup", new PayloadFSHangup(_uuid, _sessionId));
            log.warn("[{}]: playWelcome: ai_reply error, hangup, detail: {}", sessionId, ExceptionUtil.exception2detail(ex));
        }
    }

    private boolean doPlayback(final AIReplyVO replyVO) {
        if (replyVO.getVoiceMode() == null || replyVO.getAi_content_id() == null) {
            return false;
        }

        final String newPlaybackId = UUID.randomUUID().toString();
        final String ai_content_id = Long.toString(replyVO.getAi_content_id());
        log.info("[{}]: doPlayback: {}", sessionId, replyVO);

        /*
        final String file = reply2rms(replyVO,
                ()->String.format("%s=%s,content_id=%s,vars_start_timestamp=%d,playback_idx=%d",
                        PLAYBACK_ID_NAME, newPlaybackId, ai_content_id, System.currentTimeMillis() * 1000L, 0),
                newPlaybackId);

        if (file != null) {
            final String prevPlaybackId = _currentPlaybackId.getAndSet(null);
            if (prevPlaybackId != null) {
                _sendEvent.accept("FSStopPlayback", new PayloadFSChangePlayback(_uuid, prevPlaybackId));
            }
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
        }*/
        return true;
    }

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
            public void onSentenceEnd(PayloadSentenceEnd payload) {
                log.info("afs_io => onSentenceEnd: {}", payload);
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
            }
        });
    }

    public void transmit(final ByteBuffer buffer, final long recvdInMs) {
        final byte[] byte8 = new byte[8];
        buffer.get(byte8, 0, 8);
        // 将小端字节序转换为 long
        final long startInMss =
                ((byte8[7] & 0xFFL) << 56) |  // 最高有效字节（小端的最后一个字节）
                        ((byte8[5] & 0xFFL) << 40) |
                        ((byte8[6] & 0xFFL) << 48) |
                        ((byte8[4] & 0xFFL) << 32) |
                        ((byte8[3] & 0xFFL) << 24) |
                        ((byte8[2] & 0xFFL) << 16) |
                        ((byte8[1] & 0xFFL) << 8)  |
                        (byte8[0] & 0xFFL);          // 最低有效字节（小端的第一个字节）

        final var operator = opRef.get();
        if (operator != null) {
            final byte[] pcm = new byte[buffer.remaining()];
            buffer.get(pcm);
            operator.transmit(pcm);
        }

        final long nowInMs = System.currentTimeMillis();
        log.info("afs_io => localIdx: {}/recvd delay: {} ms/process delay: {} ms",
                localIdx, recvdInMs - startInMss / 1000L, nowInMs - startInMss / 1000L);
    }

    public void close() {
        final ASROperator operator = opRef.getAndSet(null);
        if (null != operator) {
            operator.close();
        }
    }
}
