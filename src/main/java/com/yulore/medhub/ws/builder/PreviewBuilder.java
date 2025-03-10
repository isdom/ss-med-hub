package com.yulore.medhub.ws.builder;

import com.alibaba.nls.client.protocol.OutputFormatEnum;
import com.alibaba.nls.client.protocol.SampleRateEnum;
import com.aliyun.oss.OSS;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.mgnt.utils.StringUnicodeEncoderDecoder;
import com.yulore.bst.*;
import com.yulore.medhub.api.CallApi;
import com.yulore.medhub.api.CompositeVO;
import com.yulore.medhub.api.ScriptApi;
import com.yulore.medhub.service.ASRService;
import com.yulore.medhub.service.BSTService;
import com.yulore.medhub.service.TTSService;
import com.yulore.medhub.session.PreviewSession;
import com.yulore.medhub.task.PlayStreamPCMTask;
import com.yulore.medhub.task.PlayStreamPCMTask2;
import com.yulore.medhub.task.SampleInfo;
import com.yulore.medhub.vo.*;
import com.yulore.medhub.ws.WsHandler;
import com.yulore.medhub.ws.WsHandlerBuilder;
import com.yulore.medhub.ws.actor.PoActor;
import com.yulore.medhub.ws.actor.PreviewActor;
import com.yulore.util.ExceptionUtil;
import com.yulore.util.VarsUtil;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@RequiredArgsConstructor
@Component("preview_io")
@ConditionalOnProperty(prefix = "feature", name = "preview_io", havingValue = "enabled")
public class PreviewBuilder implements WsHandlerBuilder {
    @Override
    public WsHandler build(final String prefix, final WebSocket webSocket, final ClientHandshake handshake) {
        final var actor = new PreviewActor() {
            @Override
            public void onMessage(WebSocket webSocket, String message) {
                try {
                    handleCommand(new ObjectMapper().readValue(message, WSCommandVO.class), webSocket, this);
                } catch (JsonProcessingException ex) {
                    log.error("handleHubCommand {}: {}, an error occurred when parseAsJson: {}",
                            webSocket.getRemoteSocketAddress(), message, ExceptionUtil.exception2detail(ex));
                }
            }

            @Override
            public void onMessage(WebSocket webSocket, ByteBuffer bytes) {
            }
        };
        webSocket.setAttachment(actor);
        return actor;
    }

    private void handleCommand(final WSCommandVO cmd, final WebSocket webSocket, final PreviewActor actor) {
        if ("Preview".equals(cmd.getHeader().get("name"))) {
            handlePreviewCommand(cmd, webSocket, actor);
        } else {
            log.warn("handleCommand: Unknown Command: {}", cmd);
        }
    }

    private void handlePreviewCommand(final WSCommandVO cmd, final WebSocket webSocket, final PreviewActor actor) {
        final String cps = URLDecoder.decode(cmd.getPayload().get("cps"), Charsets.UTF_8);

        if (actor.isPlaying()) {
            log.error("Preview: {} has already playing, ignore: {}", actor, cps);
            return;
        }

        previewOn(String.format("type=cp,%s", cps), actor, webSocket);
    }

    private void previewOn(final String path, final PreviewActor actor, final WebSocket webSocket) {
        // interval = 20 ms
        int interval = 20;
        log.info("previewOn: {} => sample rate: {}/interval: {}/channels: {}", path, 16000, interval, 1);
        final PlayStreamPCMTask task = new PlayStreamPCMTask(
                "preview",
                path,
                schedulerProvider.getObject(),
                new SampleInfo(16000, interval, 16, 1),
                (ignore)->{},
                (ignore)->{},
                webSocket::send,
                (_task) -> {
                    log.info("previewOn PlayStreamPCMTask {} stopped with completed: {}", _task, _task.isCompleted());
                    webSocket.close(1000, "close");
                }
        );
        final BuildStreamTask bst = bstService.getTaskOf(path, true, 16000);
        if (bst != null) {
            actor.attach(task);
            actor.notifyPlaybackStart(task);
            bst.buildStream(task::appendData, (ignore)->task.appendCompleted());
        }
    }

    @Autowired
    private BSTService bstService;

    private final ObjectProvider<ScheduledExecutorService> schedulerProvider;
}
