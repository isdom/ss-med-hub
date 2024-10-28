package com.yulore.aliyun;

import com.aliyun.openservices.log.common.auth.Credentials;
import com.aliyun.openservices.log.common.auth.CredentialsProvider;

public class SLSCredentialsProvider implements CredentialsProvider {
    public SLSCredentialsProvider(String accessKeyId, String accessKeySecret) {
        this._accessKeyId = accessKeyId;
        this._accessKeySecret = accessKeySecret;
    }

    /**
     * @return
     */
    @Override
    public Credentials getCredentials() {
        return new Credentials() {
            /**
             * @return
             */
            @Override
            public String getAccessKeyId() {
                return _accessKeyId;
            }

            /**
             * @return
             */
            @Override
            public String getAccessKeySecret() {
                return _accessKeySecret;
            }

            /**
             * @return
             */
            @Override
            public String getSecurityToken() {
                return "";
            }

            /**
             * @param s
             */
            @Override
            public void setAccessKeyId(String s) {

            }

            /**
             * @param s
             */
            @Override
            public void setAccessKeySecret(String s) {

            }

            /**
             * @param s
             */
            @Override
            public void setSecurityToken(String s) {

            }
        };
    }

    private String _accessKeyId;
    private String _accessKeySecret;
}
