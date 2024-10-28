package com.yulore.aliyun;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import com.aliyun.openservices.log.common.auth.CredentialsProvider;
import com.aliyun.openservices.log.logback.CredentialsProviderBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.StringReader;
import java.util.Properties;

@Slf4j
public class SLSCPBuilder implements CredentialsProviderBuilder {
    @Override
    public CredentialsProvider getCredentialsProvider() throws Exception {
        String accessKeyId = null;
        String accessKeySecret = null;
        try {
            final String serverAddr = System.getenv("NACOS_ENDPOINT");
            final String namespace = System.getenv("NACOS_NAMESPACE");

            // https://help.aliyun.com/zh/mse/use-cases/migrate-configurations-from-a-self-managed-nacos-configuration-center-to-an-mse-nacos-instance
            final Properties properties = new Properties();
            properties.put("serverAddr", serverAddr);
            properties.put("namespace", namespace);

            final ConfigService configService = NacosFactory.createConfigService(properties);
            final String content = configService.getConfig(this._dataId, this._group, 5000);

            final Properties credentials = new Properties();
            credentials.load(new StringReader(content));

            accessKeyId = credentials.get("accessKeyId").toString();
            accessKeySecret = credentials.get("accessKeySecret").toString();

            log.info("MSE {}/{}/{}/{} => content: {}\naccessKeyId:{}\naccessKeySecret:{}",
                    serverAddr, namespace, this._group, this._dataId, content,
                    accessKeyId, accessKeySecret);
        } catch (NacosException e) {
            log.error("getCredentialsProvider: {}", e.toString());
        }

        return new SLSCredentialsProvider(accessKeyId, accessKeySecret);
    }

    private String _dataId;
    private String _group;

    // 自定义参数 dataId
    public void setDataId(final String dataId) {
        this._dataId = dataId;
    }
    // 自定义参数 group
    public void setGroup(final String group) {
        this._group = group;
    }
}
