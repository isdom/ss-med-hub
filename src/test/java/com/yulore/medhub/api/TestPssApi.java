package com.yulore.medhub.api;

import com.yulore.funasr.TestFunasr;
import feign.Feign;
import feign.Logger;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.slf4j.Slf4jLogger;
import org.springframework.cloud.openfeign.support.SpringMvcContract;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Properties;
import java.util.stream.Stream;

public class TestPssApi {
    public static void main(String[] args) throws Exception {
        final Properties props = new Properties();

        try (var configStream = TestFunasr.class.getClassLoader().getResourceAsStream("test-pss.properties")) {
            props.load(configStream);
        } catch (Exception e) {
            throw new RuntimeException("加载配置文件失败，请确保 test-pss.properties 存在且路径正确", e);
        }

        // 1. 读取音频文件并编码为 Base64
        final String audioPath = props.getProperty("wav.path");
        final String pssUrl = props.getProperty("pss.url");

        byte[] audioBytes = Files.readAllBytes(Paths.get(audioPath));
        final var base64Audio = Base64.getEncoder().encodeToString(audioBytes);

        // 2. 创建请求对象
        final var request = PssApi.SpkRequest.builder().audio(base64Audio).build();

        // 3. 创建 Feign 客户端
        final var pssApi = Feign.builder()
                .contract(new SpringMvcContract())
                //.client(new OkHttpClient())
                .encoder(new JacksonEncoder())
                .decoder(new JacksonDecoder())
                .logger(new Slf4jLogger(TestPssApi.class))
                .logLevel(Logger.Level.FULL)
                .target(PssApi.class, pssUrl);

        // 4. 发送请求
        PssApi.SpkResponse response = pssApi.vector_spk(request);

        // 5. 处理响应
        System.out.println("请求成功: " + response.isSuccess());
        System.out.println("状态码: " + response.getCode());
        System.out.println("向量长度: " + response.getResult().vec.size());

        // 打印前5个向量值
        System.out.println("向量示例: ");
        Stream.of(response.getResult().vec)
                .limit(5)
                .forEach(System.out::println);
    }
}
