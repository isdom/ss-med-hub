package com.yulore.aliyun.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OSSConfig {

    @Value("${aliyun.oss.endpoint}")
    private String ossEndpoint;

    @Value("${aliyun.oss.access_key_id}")
    private String ossAccessKeyId;

    @Value("${aliyun.oss.access_key_secret}")
    private String ossAccessKeySecret;

    @Bean
    public OSS ossClient() {
        // 创建并返回 OSS 客户端实例
        return new OSSClientBuilder().build(ossEndpoint, ossAccessKeyId, ossAccessKeySecret);
    }
}
