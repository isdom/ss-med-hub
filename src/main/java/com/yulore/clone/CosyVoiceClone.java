package com.yulore.clone;

import com.aliyuncs.CommonRequest;
import com.aliyuncs.CommonResponse;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.exceptions.ServerException;
import com.aliyuncs.http.MethodType;
import com.aliyuncs.http.ProtocolType;
import com.aliyuncs.profile.DefaultProfile;

public class CosyVoiceClone {
    //域名
    private static final String DOMAIN = "nls-slp.cn-shanghai.aliyuncs.com";
    // API版本
    private static final String API_VERSION = "2019-08-19";

    private static final IAcsClient client;

    static {
        // 创建DefaultAcsClient实例并初始化
        DefaultProfile profile = DefaultProfile.getProfile(
                "cn-shanghai",
                // 通过环境变量获取阿里云Ak ID，避免使用明文造成泄露风险
                // 如您未配置环境变量，请将下行代码中的System.getenv("AK_ID")替换为AK ID
                "<ak_id>", //System.getenv("AK_ID"),
                // 通过环境变量获取阿里云Ak Secret, 避免使用明文造成泄露风险
                // 如您未配置环境变量，请将下行代码中的System.getenv("AK_SECRET"))替换为AK Secret
                "<ak_secret>>"//System.getenv("AK_SECRET")
                );
        client = new DefaultAcsClient(profile);
    }

    public static void main(String[] args) throws InterruptedException {
        String voicePrefix = "prefix_for_voice";
        String url = "http://record.wav";
        cosyClone(voicePrefix, url);
        cosyList(voicePrefix);
    }

    private static void cosyList(String voicePrefix) {
        CommonRequest request = buildRequest("ListCosyVoice");
        request.putBodyParameter("VoicePrefix", voicePrefix);
        String response = sendRequest(request);
        System.out.println(response);
    }

    private static void cosyClone(String voicePrefix, String url) {
        CommonRequest cloneRequest = buildRequest("CosyVoiceClone");
        cloneRequest.putBodyParameter("VoicePrefix", voicePrefix);
        cloneRequest.putBodyParameter("Url", url);
        // 设定等待超时时间为15s
        cloneRequest.setSysReadTimeout(15000);
        long startTime = System.currentTimeMillis();
        String response = sendRequest(cloneRequest);
        long endTime = System.currentTimeMillis();
        System.out.println(response);
        System.out.println("cost: "+ (endTime - startTime) + " milliseconds");
    }

    private static CommonRequest buildRequest(String popApiName) {
        CommonRequest request = new CommonRequest();
        request.setMethod(MethodType.POST);
        request.setDomain(DOMAIN);
        request.setVersion(API_VERSION);
        request.setAction(popApiName);
        request.setProtocol(ProtocolType.HTTPS);
        return request;
    }

    private static String sendRequest(CommonRequest request) {
        try {
            CommonResponse response = client.getCommonResponse(request);
            return response.getData();
        } catch (ServerException e) {
            e.printStackTrace();
        } catch (ClientException e) {
            e.printStackTrace();
        }
        return null;
    }
}