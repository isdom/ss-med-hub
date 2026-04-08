package com.yulore.aliyun.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.yulore.util.ExceptionUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.search.BitapSearchProcessorFactory;
import io.netty.buffer.search.SearchProcessorFactory;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.base64.Base64;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.ByteProcessor;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import javax.net.ssl.SSLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

// REF: https://help.aliyun.com/zh/model-studio/interactive-process-of-qwen-tts-realtime-synthesis
// Model: https://help.aliyun.com/zh/model-studio/qwen-tts-realtime
// URL: 中国内地：wss://dashscope.aliyuncs.com/api-ws/v1/realtime
@Slf4j
public class QwenTTSWSClient {
    // https://help.aliyun.com/zh/model-studio/qwen-tts-realtime-server-events
    @Data
    @ToString
    public static class Event {
        // 服务端事件ID。
        public String event_id;
        // 事件类型。
        public String type;
    }

    @Data
    @ToString
    public static class ErrorDetail {
        // 服务端事件ID。
        public String code;
        // 事件类型。
        public String message;
    }

    // 不论是遇到客户端错误还是服务端错误，服务端都会响应该事件。
    @Data
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    public static class ErrorEvent extends Event {
        // 错误详情。
        public ErrorDetail error;
    }

    @Data
    @ToString
    public static class SessionMeta {
        // 会话ID。
        public String id;
        // 会话服务名。
        public String object;
        // 交互模式，server_commit或commit。
        public String mode;
        // 使用的模型。
        public String model;
        // 使用的音色。
        public String voice;
        // 音频格式。
        public String response_format;
        // 音频采样率。
        public int sample_rate;
        // 音频语种。
        public String language_type;
    }

    @Data
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    public static class SessionCreated extends Event {
        // 会话配置。
        public SessionMeta session;
    }

    // 接收到客户端的session.update请求并正确处理后返回。如果出现错误，则直接返回error事件。
    @Data
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    public static class SessionUpdated extends Event {
        // 会话配置。
        public SessionMeta session;
    }

    // 事件类型，固定为session.finished。
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    public static class SessionFinished extends Event {
    }

    // 事件类型，固定为input_text_buffer.committed。
    @Data
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    public static class InputTextBufferCommitted extends Event {
        // 将创建的用户消息项的 ID。
        public String item_id;
    }

    // 事件类型，固定为input_text_buffer.cleared。
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    public static class InputTextBufferCleared extends Event {
    }

    @Data
    @ToString
    public static class Response {
        // 响应ID。
        public String id;
        // 对象类型，在此事件下固定为realtime.response。
        public String object;
        // 响应的最终状态，取值范围：completed/failed/in_progress/incomplete
        public String status;
        // 使用的音色。
        public String voice;
    }

    // 事件类型，固定为response.created。
    @Data
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    public static class ResponseCreated extends Event {
        // 响应详情。
        public Response response;
    }

    @Data
    @ToString
    public static class Item {
        // 输出项ID。
        public String id;
        // 始终为 realtime.item。
        public String object;
        // 输出项的状态。
        public String status;
        // 消息的内容。
        // public Object[] content;
    }

    // 事件类型，固定为response.output_item.added。
    @Data
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    public static class ResponseOutputItemAdded extends Event {
        // 响应的ID。
        public String response_id;
        // 响应输出项的索引，目前固定为0。
        public int output_index;
        // 响应详情。
        public Item item;
    }

    @Data
    @ToString
    public static class Part {
        // 内容部分的类型。
        public String type;
        // 内容部分的文本。
        public String text;
    }

    // 当新的内容项需要输出时，服务端返回此事件。
    // 事件类型，固定为response.output_item.added。
    @Data
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    public static class ResponseContentPartAdded extends Event {
        // 响应的ID。
        public String response_id;
        // 消息项ID。
        public String item_id;
        // 响应输出项的索引，目前固定为0。
        public int output_index;
        // 响应输出项中内部部分的索引，目前固定为0。
        public int content_index;
        // 已完成的内容部分。
        public Part part;
    }

    // 当模型增量生成新的audio数据时，系统会返回服务器 response.audio.delta 事件。
    // 事件类型，固定为response.output_item.added。
    @Data
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    public static class ResponseAudioDelta extends Event {
        // 响应的ID。
        public String response_id;
        // 消息项ID。
        public String item_id;
        // 响应输出项的索引，目前固定为0。
        public int output_index;
        // 响应输出项中内部部分的索引，目前固定为0。
        public int content_index;
        // 已完成的内容部分。
        public String delta;
    }

    // 当新的内容项需要输出时，服务端返回此事件。
    // 事件类型，固定为response.output_item.added。
    @Data
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    public static class ResponseContentPartDone extends Event {
        // 响应的ID。
        public String response_id;
        // 消息项ID。
        public String item_id;
        // 响应输出项的索引，目前固定为0。
        public int output_index;
        // 响应输出项中内部部分的索引，目前固定为0。
        public int content_index;
        // 已完成的内容部分。
        public Part part;
    }

    // 当新的item输出完成时，服务端返回此事件。
    @Data
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    public static class ResponseOutputItemDone extends Event {
        // 响应的ID。
        public String response_id;
        // 响应输出项的索引，目前固定为0。
        public int output_index;
        // 响应详情。
        public Item item;
    }

    // 当模型生成audio数据完成时，系统会返回服务器 response.audio.done 事件。
    @Data
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    public static class ResponseAudioDone extends Event {
        // 响应的ID。
        public String response_id;
        // 消息项ID。
        public String item_id;
        // 响应输出项的索引，目前固定为0。
        public int output_index;
        // 响应输出项中内部部分的索引，目前固定为0。
        public int content_index;
    }

    @Data
    @ToString
    public static class TokensDetail {
        // 文本内容总长度（Token）。
        public int text_tokens;
        // 音频内容总长度（Token）。音频转换为 Token 的规则：每1秒的音频对应 50个 Token 。若音频时长不足1秒，则按 50个 Token 计算。
        public int audio_tokens;
    }

    @Data
    @ToString
    public static class Usage {
        // Qwen3-TTS Realtime计费字符数。
        public int characters;
        // Qwen-TTS Realtime输入和输出（合成的音频）内容总长度（Token）。
        public int total_tokens;
        // Qwen-TTS Realtime输入内容总长度（Token）。
        public int input_tokens;
        // Qwen-TTS Realtime输出内容总长度（Token）。
        public int output_tokens;
        // Qwen-TTS Realtime输入内容长度（Token）详情。
        public TokensDetail input_tokens_details;
        // Qwen-TTS Realtime输出内容长度（Token）详情。
        public TokensDetail output_tokens_details;
    }

    @Data
    @ToString
    public static class DoneResponse {
        // 响应ID。
        public String id;
        // 对象类型，在此事件下固定为realtime.response。
        public String object;
        // 响应的最终状态，取值范围：completed/failed/in_progress/incomplete
        public String status;
        // 使用的音色。
        public String voice;
        // 响应的输出。
        //public Object[] output;
        // 使用的音色。
        public Usage usage;
    }

    // 当响应生成完成时，服务端会返回此事件。该事件中包含的 Response 对象将包含 Response 中的所有输出项，但不包括已返回的原始音频数据。
    // 事件类型，固定为response.done。
    @Data
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    public static class ResponseDone extends Event {
        // 响应的ID。
        public String response_id;
        // 响应详情。
        public DoneResponse response;
    }

    @Builder
    @Data
    @ToString
    public static class SessionConfig {
        // （必选）使用的音色。
        public String voice;
        // （可选）交互模式，可选值： server_commit（默认）：服务端自动判断合成时机，平衡延迟与质量，推荐大多数场景使用 / commit：客户端手动触发合成，延迟最低，但需自行管理句子完整性
        @Builder.Default
        public String mode = "server_commit";
        // （可选）指定合成音频的语种，默认为 Auto。
        //Auto：适用无法确定文本的语种或文本包含多种语言的场景，模型会自动为文本中的不同语言片段匹配各自的发音，但无法保证发音完全精准。
        //指定语种：适用于文本为单一语种的场景，此时指定为具体语种，能显著提升合成质量，效果通常优于 Auto。可选值包括：
        // Chinese、English、German、Italian、Portuguese、Spanish、Japanese、Korean、French、Russian
        @Builder.Default
        public String language_type = "Auto";
        // （可选）模型输出音频的格式。
        //支持的格式：
        //pcm（默认）、wav、mp3、opus，千问-TTS-Realtime（参见支持的模型）仅支持pcm。
        @Builder.Default
        public String response_format = "mp3";
        // （可选）模型输出音频的采样率（Hz）。
        //支持的采样率：8000、16000、24000（默认）、48000，千问-TTS-Realtime（参见支持的模型）仅支持24000。
        @Builder.Default
        public int sample_rate = 24000;
        // （可选）音频的语速。1.0为正常语速，小于1.0为慢速，大于1.0为快速。
        //默认值：1.0。 取值范围：[0.5, 2.0]。
        //千问-TTS-Realtime（参见支持的模型）不支持该参数。
        @Builder.Default
        public float speech_rate = 1.0f;
        // （可选）音频的音量。
        //默认值：50。 取值范围：[0, 100]。千问-TTS-Realtime（参见支持的模型）不支持该参数。
        @Builder.Default
        public int volume = 50;
        // （可选）合成音频的语调。
        //默认值：1.0。取值范围：[0.5, 2.0]。千问-TTS-Realtime（参见支持的模型）不支持该参数。
        @Builder.Default
        public float pitch_rate = 1.0f;
        // 可选）指定音频的码率（kbps）。码率越大，音质越好，音频文件体积越大。仅在音频格式（response_format）为opus时可用。
        //默认值：128。取值范围：[6, 510]。
        @Builder.Default
        public int bit_rate = 128;
        // （可选）设置指令，参见指令控制。
        //默认值：无默认值，不设置不生效。长度限制：长度不得超过 1600 Token。支持语言：仅支持中文和英文。适用范围：该功能仅适用于千问3-TTS-Instruct-Flash-Realtime系列模型。
        public String instructions;
        //（可选）是否对 instructions 进行优化，以提升语音合成的自然度和表现力。
        //默认值：false。
        //行为说明：当设置为 true 时，系统将对 instructions 的内容进行语义增强与重写，生成更适合语音合成的内部指令。
        //适用场景：推荐在追求高品质、精细化语音表达的场景下开启。
        //依赖关系：此参数依赖于 instructions 参数被设置。如果 instructions 为空，此参数不生效。
        //适用范围：该功能仅适用于千问3-TTS-Instruct-Flash-Realtime系列模型。
        @Builder.Default
        public boolean optimize_instructions = false;
    }

    // 用于更新会话配置。在WebSocket连接建立成功后，可立即发送此事件作为交互的第一步。如果未发送，系统将使用默认配置。服务端成功处理此事件后，会返回session.updated事件作为确认。
    @Builder
    @Data
    @ToString
    public static class SessionUpdate {
        // 服务端事件ID。
        public String event_id;
        // 事件类型，固定为session.update。
        @Builder.Default
        public String type = "session.update";
        // （可选）会话配置。
        private SessionConfig session;
    }

    // 用于将待合成文本追加到文本缓冲区。在server_commit模式中，文本将追加到服务端的文本缓冲区；在commit模式中，文本将追加到客户端的文本缓冲区。
    @Builder
    @Data
    @ToString
    public static class InputTextBufferAppend {
        // 服务端事件ID。
        public String event_id;
        // 事件类型，固定为input_text_buffer.append。
        @Builder.Default
        public String type = "input_text_buffer.append";
        // （必选）待合成文本。
        private String text;
    }

    // 用于提交用户输入文本缓冲区，从而在对话中创建新的用户消息项。 如果输入的文本缓冲区为空，此事件将产生错误。处于“server_commit”模式时，用户提交此事件，表示立即合成之前的所有文本，服务器不再缓存文本。
    // 处于“commit”模式时，客户端必须提交文本缓冲区才能创建用户消息项。提交输入文本缓冲区不会从模型创建响应，服务器将返回 input_text_buffer.committed 事件进行响应。
    @Builder
    @Data
    @ToString
    public static class InputTextBufferCommit {
        // 服务端事件ID。
        public String event_id;
        // 事件类型，固定为input_text_buffer.commit。
        @Builder.Default
        public String type = "input_text_buffer.commit";
    }

    // 用于清除缓冲区中的文本。服务端返回input_text_buffer.cleared 事件进行响应。
    @Builder
    @Data
    @ToString
    public static class InputTextBufferClear {
        // 服务端事件ID。
        public String event_id;
        // 事件类型，固定为input_text_buffer.clear。
        @Builder.Default
        public String type = "input_text_buffer.clear";
    }

    // 客户端发送 session.finish 事件通知服务端不再有文本输入，服务端将剩余音频返回，随后关闭连接。
    @Builder
    @Data
    @ToString
    public static class SessionFinish {
        // 服务端事件ID。
        public String event_id;
        // 事件类型，固定为session.finish。
        @Builder.Default
        public String type = "session.finish";
    }

    public interface SynthesizerOp {
        void text(String text);
        void finish();
    }

    public interface BeforeSessionUpdate extends Consumer<SessionConfig> {}
    public interface OnStart extends Consumer<SynthesizerOp> {}
    public interface OnBinary extends Consumer<byte[]> {}
    public interface OnStop extends Consumer<String> {}

    private final BeforeSessionUpdate _beforeSessionUpdate;
    private final OnStart _onStart;
    private final OnBinary _onBinary;
    private final OnStop _onStop;


    private String _apiKey;
    private long _startInMs;
    private long _firstTextInMs = 0;

    private interface UserEventHandler extends Consumer<Object> {}
    private interface BinaryHandler extends Consumer<ByteBuf> {}
    private interface TextHandler extends Consumer<TextWebSocketFrame> {}

    @RequiredArgsConstructor
    private static class OnHandshakeComplete implements UserEventHandler {
        private final Runnable _action;
        @Override
        public String toString() {
            return "ON_HANDSHAKE_COMPLETE";
        }
        @Override
        public void accept(final Object evt) {
            if (evt == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
                _action.run();
            }
        }
    }

    private static final Map<String, Class<? extends Event>> _EVENT2CLS = new HashMap<>();
    {
        _EVENT2CLS.put("error", ErrorEvent.class);
        _EVENT2CLS.put("session.created", SessionCreated.class);
        _EVENT2CLS.put("session.updated", SessionUpdated.class);
        _EVENT2CLS.put("session.finished", SessionFinished.class);
        _EVENT2CLS.put("input_text_buffer.committed", InputTextBufferCommitted.class);
        _EVENT2CLS.put("input_text_buffer.cleared", InputTextBufferCleared.class);
        _EVENT2CLS.put("response.created", ResponseCreated.class);
        _EVENT2CLS.put("response.done", ResponseDone.class);
        _EVENT2CLS.put("response.output_item.added", ResponseOutputItemAdded.class);
        _EVENT2CLS.put("response.output_item.done", ResponseOutputItemDone.class);
        _EVENT2CLS.put("response.content_part.added", ResponseContentPartAdded.class);
        _EVENT2CLS.put("response.content_part.done", ResponseContentPartDone.class);
        _EVENT2CLS.put("response.audio.delta", ResponseAudioDelta.class);
        _EVENT2CLS.put("response.audio.done", ResponseAudioDone.class);
    }

    private static final byte[] EVENT_ID_BYTES = "\"event_id\"".getBytes(StandardCharsets.UTF_8);
    private static final SearchProcessorFactory SEARCH_EVENT_ID = BitapSearchProcessorFactory.newBitapSearchProcessorFactory(EVENT_ID_BYTES);

    private static final byte[] TYPE_BYTES = "\"type\"".getBytes(StandardCharsets.UTF_8);
    private static final SearchProcessorFactory SEARCH_TYPE = BitapSearchProcessorFactory.newBitapSearchProcessorFactory(TYPE_BYTES);

    private static final byte[] DELTA_BYTES = "\"delta\"".getBytes(StandardCharsets.UTF_8);
    private static final SearchProcessorFactory SEARCH_DELTA = BitapSearchProcessorFactory.newBitapSearchProcessorFactory(DELTA_BYTES);

    private static final ByteProcessor.IndexOfProcessor INDEX_OF_QUOTES = new ByteProcessor.IndexOfProcessor((byte)'"');

    private static String extractValue(final ByteBuf content, final ByteProcessor findKey) {
        int keyEnd = content.forEachByte(findKey);
        if (keyEnd != -1) {
            final var idx = keyEnd + 1;
            final var quotesBegin = content.forEachByte(idx, content.readableBytes() - idx, INDEX_OF_QUOTES);
            if (quotesBegin != -1) {
                final var quotesEnd = content.forEachByte( quotesBegin + 1, content.readableBytes() - quotesBegin - 1, INDEX_OF_QUOTES);
                if (quotesEnd != -1) {
                    final var bytes = new byte[quotesEnd - quotesBegin - 1];
                    content.getBytes(quotesBegin+1, bytes);
                    return new String(bytes, StandardCharsets.UTF_8);
                }
            }
        }
        return null;
    }

    private static ByteBuf extractRaw(final ByteBuf content, final ByteProcessor findKey) {
        int keyEnd = content.forEachByte(findKey);
        if (keyEnd != -1) {
            final var idx = keyEnd + 1;
            final var quotesBegin = content.forEachByte(idx, content.readableBytes() - idx, INDEX_OF_QUOTES);
            if (quotesBegin != -1) {
                final var quotesEnd = content.forEachByte( quotesBegin + 1, content.readableBytes() - quotesBegin - 1, INDEX_OF_QUOTES);
                if (quotesEnd != -1) {
                    return content.slice(quotesBegin+1, quotesEnd - quotesBegin - 1);
                }
            }
        }
        return null;
    }

    private final TextHandler ON_TASK_STARTED = new TextHandler() {
        @Override
        public String toString() {
            return "ON_TASK_STARTED";
        }
        @Override
        public void accept(final TextWebSocketFrame frame) {
            // search "event_id"
            final var event_id = extractValue(frame.content(), SEARCH_EVENT_ID.newSearchProcessor());

            if (event_id == null) {
                log.warn("event_id is null, ignore");
                return;
            }

            // search "type"
            final var type = extractValue(frame.content(), SEARCH_TYPE.newSearchProcessor());
            if (type == null) {
                log.warn("event: {} missing event field, ignore", event_id);
                return;
            }

            final var cls = _EVENT2CLS.get(type);
            if (cls == null) {
                log.warn("event: {} unknown type: {}, ignore", event_id, type);
                return;
            } else {
                log.info("event: {} / type: {} => cls: {}", event_id, type, cls);
            }

            switch (type) {
                case "session.created" -> {
                    log.info("session: {} created", event_id);
                    try {
                        final String msg = frame.text();
                        final var event = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).readValue(msg, cls);
                        log.info("session.created => {}", event);
                    } catch (JsonProcessingException ex) {
                        log.warn("session.created with exception: {}", ExceptionUtil.exception2detail(ex));
                    }
                    final var sessionConfig = SessionConfig.builder().build();
                    if (_beforeSessionUpdate != null) {
                        _beforeSessionUpdate.accept(sessionConfig);
                    }
                    final var sessionUpdate = SessionUpdate.builder().event_id(UUID.randomUUID().toString()).session(sessionConfig).build();
                    sendMessage(vo2string(sessionUpdate));
                }
                case "session.updated" -> {
                    log.info("session: {} updated", event_id);
                    try {
                        final String msg = frame.text();
                        final var event = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).readValue(msg, cls);
                        log.info("session.updated => {}", event);
                    } catch (JsonProcessingException ex) {
                        log.warn("session.updated with exception: {}", ExceptionUtil.exception2detail(ex));
                    }
                    _onStart.accept(new SynthesizerOp() {
                        @Override
                        public void text(final String text) {
                            if (_firstTextInMs == 0) {
                                _firstTextInMs = System.currentTimeMillis();
                            }
                            // https://help.aliyun.com/zh/model-studio/qwen-tts-realtime-client-events
                            final var append = InputTextBufferAppend.builder().event_id(UUID.randomUUID().toString()).text(text).build();
                            sendMessage(vo2string(append)).whenComplete((ignore, ex) -> {
                                sendMessage(vo2string(InputTextBufferCommit.builder().event_id(UUID.randomUUID().toString()).build()));
                            });
                        }

                        @Override
                        public void finish() {
                            // https://help.aliyun.com/zh/model-studio/qwen-tts-realtime-client-events
                            sendMessage(vo2string(SessionFinish.builder().event_id(UUID.randomUUID().toString()).build()));
                        }
                    });

                }
                case "response.audio.delta" -> {
                    final var content_delta = extractRaw(frame.content(), SEARCH_DELTA.newSearchProcessor());
                    if (content_delta != null) {
                        log.info("response.audio.delta => audio_base64_length:{}", content_delta.readableBytes());
                        if (_firstTextInMs != -1) {
                            log.info("first audio slice cost: {} ms", System.currentTimeMillis() - _firstTextInMs);
                            _firstTextInMs = -1;
                        }

                        ByteBuf decoded = null;
                        try {
                            decoded = Base64.decode(content_delta);
                            if (_onBinary != null && decoded != null) {
                                final var bytes = new byte[decoded.readableBytes()];
                                decoded.readBytes(bytes);
                                _onBinary.accept(bytes);
                            }
                        } finally {
                            if (decoded != null) {
                                decoded.release(); // 在 finally 块中确保释放
                            }
                        }
                    } else {
                        log.warn("response.audio.delta missing key delta, ignore");
                    }
                    /*
                    try {
                        final String msg = frame.text();
                        final var event = (ResponseAudioDelta)new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).readValue(msg, cls);
                        log.info("response.audio.delta => response_id:{} / item_id: {} / audio_base64_length:{}", event.response_id, event.item_id, event.delta.length());
                        if (_firstTextInMs != -1) {
                            log.info("first audio slice cost: {} ms", System.currentTimeMillis() - _firstTextInMs);
                            _firstTextInMs = -1;
                        }

                        if (_onBinary != null) {
                            _onBinary.accept(Base64.getDecoder().decode(event.delta));
                        }
                    } catch (JsonProcessingException ex) {
                        log.warn("response.audio.delta with exception: {}", ExceptionUtil.exception2detail(ex));
                    }
                    */
                }
                case "session.finished" -> {
                    if (_onStop != null) {
                        _onStop.accept("OK");
                    }
                }
                default -> {
                    try {
                        final String msg = frame.text();
                        final var event = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).readValue(msg, cls);
                        log.info("event => {}", event);
                    } catch (JsonProcessingException ex) {
                        log.warn("event with exception: {}", ExceptionUtil.exception2detail(ex));
                    }
                }
            }
        }
    };

    private final AtomicReference<UserEventHandler> refUserEventHandler = new AtomicReference<>();
    private final AtomicReference<BinaryHandler> refBinaryHandler = new AtomicReference<>();
    private final AtomicReference<TextHandler> refTextHandler = new AtomicReference<>();

    private URI uri;
    private Channel channel;
    private final EventLoopGroup group;

    public QwenTTSWSClient(final EventLoopGroup group,
                           final String url,
                           final String apiKey,
                           final BeforeSessionUpdate beforeSessionUpdate,
                           final OnStart onStart,
                           final OnBinary onBinary,
                           final OnStop onStop
    ) {
        this.uri = buildUri(url);
        this.group = group;
        this._apiKey = apiKey;
        this._beforeSessionUpdate = beforeSessionUpdate;
        this._onStart = onStart;
        this._onBinary = onBinary;
        this._onStop = onStop;

        this._startInMs = System.currentTimeMillis();
        connect().whenComplete((channel, ex)-> {
            if (ex != null) {
                // TODO:
                log.warn("qwentts_connect_to {} failed: {}", url, ExceptionUtil.exception2detail(ex));
            } else {
                this.channel = channel;
                final long cost = System.currentTimeMillis() - _startInMs;
                log.info("qwentts_connect_to {} cost: {} ms", url, cost);
            }
        });
    }

    private static URI buildUri(final String url) {
        try {
            return new URI(url);
        } catch (URISyntaxException e) {
            return null;
        }
    }

    private int getPort() {
        int port = uri.getPort();
        return port == -1 ? ("wss".equals(uri.getScheme()) ? 443 : 80) : port;
    }

    // 客户端初始化与连接
    private CompletionStage<Channel> connect() {
        changeUserEventHandler(new OnHandshakeComplete(() -> {
            changeTextHandler(ON_TASK_STARTED);
        }));

        final var result = new CompletableFuture<Channel>();

        new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true) // 禁用Nagle算法
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT) // 使用内存池
                .handler(new LoggingHandler(LogLevel.DEBUG)) // 生产环境可关闭
                .handler(new ClientInitializer())
                .connect(uri.getHost(), getPort()).addListener(future -> {
                    if (!future.isSuccess()) {
                        result.completeExceptionally(future.cause());
                    } else {
                        result.complete(((ChannelFuture)future).channel());
                    }
                });

        return result;
    }

    private String vo2string(final Object vo) {
        try {
            return new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS).writeValueAsString(vo);
        } catch (JsonProcessingException ex) {
            log.warn("vo2string with exception: {}", ExceptionUtil.exception2detail(ex));
        }
        return "(null)";
    }

    private void changeUserEventHandler(final UserEventHandler newHandler) {
        final var old = refUserEventHandler.getAndSet(newHandler);
        log.info("changeUserEventHandler: {} => {}", old, newHandler);
    }

    private void changeTextHandler(final TextHandler newHandler) {
        final var old = refTextHandler.getAndSet(newHandler);
        log.info("changeTextHandler: {} => {}", old, newHandler);
    }

    private void changeBinaryHandler(final BinaryHandler newHandler) {
        final var old = refBinaryHandler.getAndSet(newHandler);
        log.info("changeBinaryHandler: {} => {}", old, newHandler);
    }

    // send text data
    private CompletionStage<Void> sendMessage(final String message) {
        final var result = new CompletableFuture<Void>();

        if (channel != null && channel.isActive()) {
            final var frame = new TextWebSocketFrame(message);
            channel.writeAndFlush(frame).addListener(future -> {
                if (!future.isSuccess()) {
                    log.warn("send text frame:[{}] failed", message, future.cause());
                    result.completeExceptionally(future.cause());
                } else {
                    result.complete(null);
                }
            });
        } else {
            result.completeExceptionally(new RuntimeException("Not Connected"));
        }
        return result;
    }

    // send binary data
    private void sendBinary(final byte[] bytes) {
        if (channel != null && channel.isActive()) {
            final var frame = new BinaryWebSocketFrame(Unpooled.wrappedBuffer(bytes));
            channel.writeAndFlush(frame).addListener(future -> {
                if (!future.isSuccess()) {
                    log.warn("send binary frame:[{}] bytes failed", bytes.length, future.cause());
                }
            });
        }
    }

    // 关闭连接
    public void shutdown() {
        log.info("all cost {} ms", System.currentTimeMillis() - _startInMs);
        if (channel != null) {
            channel.close();
        }
    }

    // 管道初始化器
    private class ClientInitializer extends ChannelInitializer<SocketChannel> {
        @Override
        protected void initChannel(SocketChannel ch) {
            final ChannelPipeline pipeline = ch.pipeline();

            // WebSocket协议处理器配置
            final WebSocketClientProtocolHandler wsHandler = new WebSocketClientProtocolHandler(
                    uri,
                    WebSocketVersion.V13,
                    null,
                    false,
                    new DefaultHttpHeaders()
                            .add("Authorization", "Bearer " + _apiKey)
                            .add("user-agent", "QwenTTSWSClient")
                    ,
                    1024 * 1024 // 最大内容长度 1MBytes
            );

            try {
                final boolean ssl = "wss".equalsIgnoreCase(uri.getScheme());
                if (ssl) {
                    final SslContext sslCtx = SslContextBuilder.forClient()
                            .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
                    pipeline.addLast(sslCtx.newHandler(ch.alloc(), uri.getHost(), getPort()));
                }
            } catch (SSLException e) {
                throw new RuntimeException(e);
            }

            pipeline.addLast(
                    new HttpClientCodec(),
                    new HttpObjectAggregator(65536), // 聚合HTTP请求
                    wsHandler,
                    new ClientHandler());
        }
    }

    // 业务处理器
    class ClientHandler extends SimpleChannelInboundHandler<WebSocketFrame> {
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            super.userEventTriggered(ctx, evt);
            final var handler = refUserEventHandler.get();
            if (handler != null) {
                handler.accept(evt);
            }
            log.info("cosyvoice userEventTriggered: {}", evt);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            log.info("cosyvoice connected");
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
            // 高性能处理逻辑建议：
            // 1. 使用直接内存访问
            // 2. 避免阻塞操作
            // 3. 使用异步处理
            if (frame instanceof TextWebSocketFrame) {
                final var handler = refTextHandler.get();
                if (handler != null) {
                    handler.accept((TextWebSocketFrame) frame);
                } else {
                    log.warn("missing TextHandler for frame:{}, ignore", frame);
                }
            } else if (frame instanceof BinaryWebSocketFrame) {
                final var buf = frame.content();
                final var bytes = buf.readableBytes();
                final var handler = refBinaryHandler.get();
                if (handler != null) {
                    handler.accept(buf);
                } else {
                    log.warn("non-binary handler");
                }
                log.info("receive binary message: {} bytes", bytes);
            } else if (frame instanceof PongWebSocketFrame) {
                log.info("receive pong frame");
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.warn("qwentts_exception: {}", ExceptionUtil.exception2detail(cause));
            ctx.close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            //final Consumer<RmsClient> whenDisconnect = refWhenDisconnect.getAndSet(null);
            //if (whenDisconnect != null) {
            //    whenDisconnect.accept(RmsClient.this);
            //}
            log.info("qwentts disconnect");
            // shutdown();
        }
    }
}
