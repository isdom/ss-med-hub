package com.yulore.medhub.ws;

import lombok.RequiredArgsConstructor;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
public class WsHandlerRegistry implements BeanFactoryAware  {
    private final Map<String, WsHandlerBuilder> handlerBuilders = new ConcurrentHashMap<>();
    private final WsConfigProperties configProps;
    private BeanFactory beanFactory;

    @PostConstruct
    public void init() {
        // 从配置加载路径映射
        configProps.pathMappings.forEach((path, handlerName) ->
                register(path, beanFactory.getBean(handlerName, WsHandlerBuilder.class))
        );
    }

    public void register(final String pathPrefix, final WsHandlerBuilder builder) {
        handlerBuilders.put(pathPrefix, builder);
    }

    public Optional<WsHandler> createHandler(final WebSocket ws, final ClientHandshake handshake) {
        final String path = handshake.getResourceDescriptor();
        return handlerBuilders.entrySet().stream()
                .filter(e -> path.startsWith(e.getKey()))
                .findFirst()
                .map(e -> e.getValue().build(e.getKey(), ws, handshake));
    }

    @Override
    public void setBeanFactory(@NotNull final BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
    }
}