package com.yulore.medhub.service;

import com.yulore.medhub.ws.actor.ASRActor;
import com.yulore.medhub.vo.cmd.VOStartTranscription;
import io.micrometer.core.instrument.Timer;
import org.java_websocket.WebSocket;

import java.util.concurrent.CompletionStage;

public interface ASRService {
    CompletionStage<Timer> startTranscription(final ASRActor<?> actor, final VOStartTranscription vo, final WebSocket webSocket);
    void stopTranscription(final WebSocket webSocket);
}
