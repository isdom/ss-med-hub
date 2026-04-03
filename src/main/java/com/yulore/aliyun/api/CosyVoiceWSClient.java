package com.yulore.aliyun.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yulore.funasr.vo.ASRResult;
import com.yulore.funasr.vo.StartASR;
import com.yulore.funasr.vo.StopASR;
import com.yulore.util.ExceptionUtil;
import io.micrometer.core.instrument.Timer;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

// REF: https://help.aliyun.com/zh/model-studio/cosyvoice-websocket-api
@Slf4j
public class CosyVoiceWSClient {
    @Builder
    @Data
    @ToString
    public static class TaskHeader {
        public String action;
        public String task_id;
        // 是否必选: 是
        // 固定字符串："duplex"
        @Builder.Default
        public String streaming = "duplex";
    }

    @Builder
    @Data
    @ToString
    public static class RunParameters {
        // 是否必选: 是
        // 固定字符串：“PlainText”。
        @Builder.Default
        public String text_type = "PlainText";

        // 是否必选: 是
        // 语音合成所使用的音色。
        //支持系统音色和复刻音色：
        //系统音色：参见音色列表。
        //复刻音色：通过声音复刻功能定制。使用复刻音色时，请确保声音复刻与语音合成使用同一账号。
        //使用声音复刻生成的复刻音色时，本请求的model参数值，必须与创建该音色时所用的模型版本（即target_model参数）完全一致。
        //声音设计音色：通过声音设计功能定制。使用声音设计音色时，请确保声音设计与语音合成使用同一账号。
        //使用声音设计生成的音色时，本请求的model参数值，必须与创建该音色时所用的模型版本（即target_model参数）完全一致。
        // REF-DOC: https://help.aliyun.com/zh/model-studio/cosyvoice-voice-list
        @Builder.Default
        public String voice = "longanhuan"; // 名称：龙安欢 / voice参数：longanhuan / 特质：欢脱元气女 / 年龄：20~30岁 / 语言：中文（普通话）、英文

        //音频编码格式。
        //所有模型均支持的编码格式：pcm、wav和mp3（默认）
        //除cosyvoice-v1外，其他模型支持的编码格式：opus
        //音频格式为opus时，支持通过bit_rate参数调整码率。
        @Builder.Default
        public String format = "mp3";
        //音频采样率（单位：Hz）。
        //默认值：22050。
        //取值范围：8000, 16000, 22050, 24000, 44100, 48000。
        //说明
        //默认采样率代表当前音色的最佳采样率，缺省条件下默认按照该采样率输出，同时支持降采样或升采样。
        public Integer sample_rate;
        // 音量。
        //默认值：50。
        //取值范围：[0, 100]。50代表标准音量。音量大小与该值呈线性关系，0为静音，100为最大
        public Integer volume;
        // 语速。
        //默认值：1.0。
        //取值范围：[0.5, 2.0]。1.0为标准语速，小于1.0则减慢，大于1.0则加快。
        public Float rate;
        // 音高。该值作为音高调节的乘数，但其与听感上的音高变化并非严格的线性或对数关系，建议通过测试选择合适的值。
        //默认值：1.0。
        //取值范围：[0.5, 2.0]。1.0为音色自然音高。大于1.0则音高变高，小于1.0则音高变低。
        public Float pitch;
        // 是否开启SSML功能。
        //该参数设为 true 后，仅允许发送一次文本（只允许发送一次continue-task指令）。
        public Boolean enable_ssml;
        // 音频码率（单位kbps）。音频格式为opus时，支持通过bit_rate参数调整码率。
        //默认值：32。
        //取值范围：[6, 510]。
        //cosyvoice-v1模型不支持该参数。
        public Integer bit_rate;
        // 是否开启字级别时间戳。
        //默认值：false。
        //true：开启。
        //false：关闭。
        //该功能仅适用于cosyvoice-v3-flash、cosyvoice-v3-plus和cosyvoice-v2模型的复刻音色，以及音色列表中标记为支持的系统音色。
        //开启 word_timestamp_enabled 后，时间戳信息会在 result-generated 事件中返回。示例如
        public Boolean word_timestamp_enabled;
        // 生成时使用的随机数种子，使合成的效果产生变化。在模型版本、文本、音色及其他参数均相同的前提下，使用相同的seed可复现相同的合成结果。
        //默认值0。
        //取值范围：[0, 65535]。
        //cosyvoice-v1不支持该功能。
        public Integer seed;
        // 设置指令，用于控制方言、情感或角色等合成效果。该功能仅适用于cosyvoice-v3.5-flash、cosyvoice-v3.5-plus和cosyvoice-v3-flash模型的复刻音色，以及音色列表中标记为支持Instruct的系统音色。
        //长度限制：100字符。
        //汉字（包括简/繁体汉字、日文汉字和韩文汉字）按2个字符计算，其他所有字符（如标点符号、字母、数字、日韩文假名/谚文等）均按 1个字符计算
        //使用要求（因模型而异）：
        //cosyvoice-v3.5-flash和cosyvoice-v3.5-plus：可以输入任意指令控制合成效果（如情感、语速等）
        //重要
        //cosyvoice-v3.5-flash和cosyvoice-v3.5-plus无系统音色，仅支持使用声音设计/复刻音色。
        //指令示例：
        //请用非常激昂且高亢的语气说话，表现出获得重大成功后的狂喜与激动。
        //语速请保持中等偏慢，语气要显得优雅、知性，给人以从容不迫的安心感。
        //语气要充满哀伤与怀念，带有轻微的鼻音，仿佛正在诉说一段令人心碎的往事。
        //请尝试用气声说话，音量极轻，营造出一种在耳边亲密低语的神秘感。
        //语气要显得非常急躁且不耐烦，语速加快，句子之间的停顿要尽量缩短。
        //请模拟一位慈祥、温和的长辈，语速平稳，声音中要透出满满的关怀与爱意。
        //语气要充满讽刺和不屑，在关键词上加重读音，句尾语调略微上扬。
        //请用一种极度恐惧且颤抖的声音说话。
        //语气要像专业的新闻播音员一样，冷静、客观且字正腔圆，情绪保持中立。
        //语气要显得活泼俏皮，带着明显的笑意，让声音听起来充满朝气与阳光。
        //cosyvoice-v3-flash：需遵照如下要求
        //复刻音色：可使用任意自然语言控制语音合成效果。
        //指令示例：
        //请用广东话表达。（支持的方言：广东话、东北话、甘肃话、贵州话、河南话、湖北话、江西话、闽南话、宁夏话、山西话、陕西话、山东话、上海话、四川话、天津话、云南话。）
        //请尽可能非常大声地说一句话。
        //请用尽可能慢地语速说一句话。
        //请用尽可能快地语速说一句话。
        //请非常轻声地说一句话。
        //你可以慢一点说吗
        //你可以非常快一点说吗
        //你可以非常慢一点说吗
        //你可以快一点说吗
        //请非常生气地说一句话。
        //请非常开心地说一句话。
        //请非常恐惧地说一句话。
        //请非常伤心地说一句话。
        //请非常惊讶地说一句话。
        //请尽可能表现出坚定的感觉。
        //请尽可能表现出愤怒的感觉。
        //请尝试一下亲和的语调。
        //请用冷酷的语调讲话。
        //请用威严的语调讲话。
        //我想体验一下自然的语气。
        //我想看看你如何表达威胁。
        //我想看看你怎么表现智慧。
        //我想看看你怎么表现诱惑。
        //我想听听用活泼的方式说话。
        //我想听听你用激昂的感觉说话。
        //我想听听用沉稳的方式说话的样子。
        //我想听听你用自信的感觉说话。
        //你能用兴奋的感觉和我交流吗？
        //你能否展示狂傲的情绪表达？
        //你能展现一下优雅的情绪吗？
        //你可以用幸福的方式回答问题吗？
        //你可以做一个温柔的情感演示吗？
        //能用冷静的语调和我谈谈吗？
        //能用深沉的方法回答我吗？
        //能用粗犷的情绪态度和我对话吗？
        //用阴森的声音告诉我这个答案。
        //用坚韧的声音告诉我这个答案。
        //用自然亲切的闲聊风格叙述。
        //用广播剧博客主的语气讲话。
        //系统音色：指令必须使用固定格式和内容，详情请参见音色列表
        public String instruction;
        // 文本热修复配置，用于自定义指定词语的发音或对待合成文本进行替换。仅cosyvoice-v3-flash复刻音色支持该功能。
        //参数介绍：
        //pronunciation：自定义发音。指定词语的拼音标注，用于纠正默认发音不准确的情况。
        //replace：文本替换。在语音合成前将指定词语替换为目标文本，替换后的文本将作为实际合成内容。
        public Object hot_fix;
        // 是否启用 Markdown 过滤。启用该功能后，系统在合成语音前自动过滤输入文本中的 Markdown 标记符号，避免将其朗读为文字内容。仅cosyvoice-v3-flash复刻音色支持该功能。
        //默认值：false。
        //取值范围：
        //true：启用Markdown过滤
        //false：禁用Markdown过滤
        public Boolean enable_markdown_filter;
    }

    @Builder
    @Data
    @ToString
    public static class Input {
        public String text;
    }

    @Builder
    @Data
    @ToString
    public static class RunPayload {
        public String task_group;
        public String task;
        public String function;
        public String model;
        public RunParameters parameters;
        public Input input;
    }
    // https://help.aliyun.com/zh/model-studio/cosyvoice-websocket-api?#12d8a57443dmz

    public interface AsrOp {
        void send(byte[] pcm);
        void stop();
    }

    public interface BeforeStart extends Consumer<StartASR> {}
    public interface OnStart extends Consumer<AsrOp> {}
    public interface OnText extends Consumer<String> {}
    public interface OnStop extends Consumer<String> {}

    private final BeforeStart _beforeStart;
    private final OnStart _onStart;
    private final OnText _onText;
    private final OnStop _onStop;


    private long _startInMs;

    private interface UserEventHandler extends Consumer<Object> {}
    private interface BinaryHandler extends Consumer<ByteBuf> {}
    private interface TextHandler extends Consumer<String> {}

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

    final StringBuilder _text = new StringBuilder();

    private final TextHandler ON_ASR_STARTED = new TextHandler() {
        @Override
        public String toString() {
            return "ON_ASR_STARTED";
        }
        @Override
        public void accept(final String msg) {
            try {
                final var vo = new ObjectMapper().readValue(msg, ASRResult.class);
                if (vo.mode.equals("2pass-online") || vo.mode.equals("online")) {
                    _text.append(vo.text);
                    // log.info("{} asr changed result: {}", this, _text);
                    if (null != _onText) {
                        _onText.accept(_text.toString());
                    }
                } else {
                    /*
                    _text.append(vo.text);
                    final String result = _text.toString();
                    _text.delete(0, _text.length());
                    // log.info("{} asr final result: {}", this, result);
                    if (null != _onText) {
                        _onText.accept(result);
                    }
                    */
                }

            } catch (JsonProcessingException ex) {
                log.warn("{} with exception: {}", this, ExceptionUtil.exception2detail(ex));
            }
        }
    };

    private final TextHandler ON_ASR_ENDING = new TextHandler() {
        @Override
        public String toString() {
            return "ON_ASR_ENDING";
        }
        @Override
        public void accept(final String msg) {
            try {
                final var vo = new ObjectMapper().readValue(msg, ASRResult.class);
                log.info("{} asr result: {}", this, vo);
            } catch (JsonProcessingException ex) {
                log.warn("{} with exception: {}", this, ExceptionUtil.exception2detail(ex));
            }
        }
    };

    private final AtomicReference<UserEventHandler> refUserEventHandler = new AtomicReference<>();
    private final AtomicReference<BinaryHandler> refBinaryHandler = new AtomicReference<>();
    private final AtomicReference<TextHandler> refTextHandler = new AtomicReference<>();

    private URI uri;
    private Channel channel;
    private final EventLoopGroup group;

    public CosyVoiceWSClient(final EventLoopGroup group,
                             final Timer connect_timer,
                             final String url,
                             final BeforeStart beforeStart,
                             final OnStart onStart,
                             final OnText onText,
                             final OnStop onStop
        ) {
        this.uri = buildUri(url);
        this.group = group;
        this._beforeStart = beforeStart;
        this._onStart = onStart;
        this._onText = onText;
        this._onStop = onStop;

        this._startInMs = System.currentTimeMillis();
        connect().whenComplete((channel, ex)-> {
            if (ex != null) {
                // TODO:
                log.warn("cosyvoice_connect_to {} failed: {}", url, ExceptionUtil.exception2detail(ex));
            } else {
                this.channel = channel;
                final long cost = System.currentTimeMillis() - _startInMs;
                if (null != connect_timer) {
                    connect_timer.record(cost, TimeUnit.MILLISECONDS);
                }
                log.info("cosyvoice_connect_to {} cost: {} ms", url, cost);
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
            changeTextHandler(ON_ASR_STARTED);
            final var startVO = StartASR.builder()
                    //.mode("2pass")
                    .mode("online")
                    .wav_name("realtime")
                    .wav_format("pcm")
                    .audio_fs(8000)
                    .is_speaking(true)
                    .chunk_size(new int[]{5, 10, 5})
                    .itn(true)
                    .build();

            if (_beforeStart != null) {
                _beforeStart.accept(startVO);
            }
            sendMessage(vo2string(startVO)).whenComplete((ok, ex)-> {
                        if (ex == null) {
                            _onStart.accept(new AsrOp() {
                                @Override
                                public void send(byte[] pcm) {
                                    sendBinary(pcm);
                                }

                                @Override
                                public void stop() {
                                    changeTextHandler(ON_ASR_ENDING);
                                    sendMessage(vo2string(StopASR.builder().is_speaking(false).build())).whenComplete((ok, ex)->{
                                        shutdown();
                                    });
                                }
                            });
                        }
            });
        }));

        final var result = new CompletableFuture<Channel>();

        new Bootstrap()
            .group(group)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.TCP_NODELAY, true) // 禁用Nagle算法
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
            .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT) // 使用内存池
            .handler(new LoggingHandler(LogLevel.INFO)) // 生产环境可关闭
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
            return new ObjectMapper().writeValueAsString(vo);
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
        // group.shutdownGracefully();
    }

    // 管道初始化器
    private class ClientInitializer extends ChannelInitializer<SocketChannel> {
        @Override
        protected void initChannel(SocketChannel ch) {
            ChannelPipeline pipeline = ch.pipeline();

            // WebSocket协议处理器配置
            final WebSocketClientProtocolHandler wsHandler = new WebSocketClientProtocolHandler(
                    uri,
                    WebSocketVersion.V13,
                    null,
                    false,
                    new DefaultHttpHeaders()
                        .add("Authorization", "Bearer <your_api_key>")
                        .add("user-agent", "xxx")
                        .add("X-DashScope-WorkSpace", "xxx"),
                    1024 * 1024 // 最大内容长度 1MBytes
            );
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
                final String text = ((TextWebSocketFrame) frame).text();
                final var handler = refTextHandler.get();
                if (handler != null) {
                    handler.accept(text);
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
            log.warn("cosyvoice_exception: {}", ExceptionUtil.exception2detail(cause));
            ctx.close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            //final Consumer<RmsClient> whenDisconnect = refWhenDisconnect.getAndSet(null);
            //if (whenDisconnect != null) {
            //    whenDisconnect.accept(RmsClient.this);
            //}
            try {
                if (_onStop != null) {
                    _onStop.accept(_text.toString());
                }
            } finally {
                log.info("cosyvoice disconnect");
            }
            // shutdown();
        }
    }
}
