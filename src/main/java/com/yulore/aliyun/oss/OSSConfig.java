package com.yulore.aliyun.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@ConditionalOnProperty(prefix = "aliyun.oss", name = "enabled", havingValue = "true")
public class OSSConfig {

    @Value("${aliyun.oss.endpoint}")
    private String ossEndpoint;

    @Value("${aliyun.oss.access_key_id}")
    private String ossAccessKeyId;

    @Value("${aliyun.oss.access_key_secret}")
    private String ossAccessKeySecret;

    @Bean
    public OSS ossClient() {
        log.info("create OSSClient by: {}", ossEndpoint);
        // 创建并返回 OSS 客户端实例
        return new OSSClientBuilder().build(ossEndpoint, ossAccessKeyId, ossAccessKeySecret);
    }
}
