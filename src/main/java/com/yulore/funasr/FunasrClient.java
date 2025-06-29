package com.yulore.funasr;

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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

// REF: https://github.com/modelscope/FunASR/blob/3445cd9652c71757507216ee68eb75699b32867c/runtime/docs/websocket_protocol_zh.md
@Slf4j
public class FunasrClient {
    public interface AsrOp {
        void send(byte[] pcm);
        void stop();
    }

    public interface OnStart extends Consumer<AsrOp> {}
    public interface OnText extends Consumer<String> {}
    public interface OnStop extends Consumer<String> {}

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

    public FunasrClient(final EventLoopGroup group,
                        final Timer connect_timer,
                        final String url,
                        final OnStart onStart,
                        final OnText onText,
                        final OnStop onStop
        ) {
        this.uri = buildUri(url);
        this.group = group;
        this._onStart = onStart;
        this._onText = onText;
        this._onStop = onStop;

        this._startInMs = System.currentTimeMillis();
        connect().whenComplete((channel, ex)-> {
            if (ex != null) {
                // TODO:
                log.warn("funasr_connect_to {} failed: {}", url, ExceptionUtil.exception2detail(ex));
            } else {
                this.channel = channel;
                final long cost = System.currentTimeMillis() - _startInMs;
                connect_timer.record(cost, TimeUnit.MILLISECONDS);
                log.info("funasr_connect_to {} cost: {} ms", url, cost);
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
            sendMessage(vo2string(StartASR.builder()
                    //.mode("2pass")
                    .mode("online")
                    .wav_name("realtime")
                    .wav_format("pcm")
                    .audio_fs(8000)
                    .is_speaking(true)
                    .chunk_size(new int[]{5,10,5})
                    .itn(true)
                    .build())).whenComplete((ok, ex)-> {
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
                    new DefaultHttpHeaders(),
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
            log.info("userEventTriggered: {}", evt);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            log.info("funasr connected");
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
            log.warn("funasr_exception: {}", ExceptionUtil.exception2detail(cause));
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
                log.info("funasr disconnect");
            }
            // shutdown();
        }
    }
}
