package com.yulore.medhub.api;

import com.yulore.funasr.TestFunasr;
import feign.Feign;
import feign.Logger;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.slf4j.Slf4jLogger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.openfeign.support.SpringMvcContract;

import java.util.Map;
import java.util.Properties;

@Slf4j
public class TestEslApi {
    public static void main(String[] args) throws Exception {
        final Properties props = new Properties();

        try (var configStream = TestFunasr.class.getClassLoader().getResourceAsStream("test-esl.properties")) {
            props.load(configStream);
        } catch (Exception e) {
            throw new RuntimeException("加载配置文件失败，请确保 test-esl.properties 存在且路径正确", e);
        }

        // 1. 读取音频文件并编码为 Base64
        final String eslUrl = props.getProperty("esl.url");
        final String xKey = props.getProperty("esl.xkey");

        // 3. 创建 Feign 客户端
        final var eslApi = Feign.builder()
                .contract(new SpringMvcContract())
                //.client(new OkHttpClient())
                .encoder(new JacksonEncoder())
                .decoder(new JacksonDecoder())
                .logger(new Slf4jLogger(TestEslApi.class))
                .logLevel(Logger.Level.FULL)
                .target(EslApi.class, eslUrl);

        final var eslHeaders = Map.of("X-Key", xKey);

        // 4. 发送请求
        final var response = eslApi.search_ref(eslHeaders, "我在上班", 0.5f);
        log.info("eslApi.search_ref resp: {}", response);
    }
}
