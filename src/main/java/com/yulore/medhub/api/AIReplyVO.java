package com.yulore.medhub.api;

import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.ToString;

import java.util.Map;

// doc: https://sxnf8icmylc.feishu.cn/docx/JeSEdJtgQoq3YJxEN3ucXi1FnFh
@Data
@ToString
public class AIReplyVO {
    // 语音文件或内定关键字
    @SerializedName("ai_speech_file")
    private String ai_speech_file = "";

    // 是否让FS挂机，1是0否
    @SerializedName("hangup")
    private int hangup = 0;

    // 回复的文本
    @SerializedName("reply_content")
    private String reply_content;

    // 语音类型，取值为tts时需处理
    @SerializedName("voiceMode")
    private String voiceMode;

    // TTS内容是否固定，内容固定时，可以按照内容为key 进行缓存
    @SerializedName("tts_fixed_content")
    private Boolean tts_fixed_content;

    // ai对话内容ID，上报数据唯一标识
    @SerializedName("ai_content_id")
    private Long ai_content_id;

    // 客户对话内容ID，上报数据唯一标识
    @SerializedName("user_content_id")
    private Long user_content_id;

    // 用来表示下发的话术输出的时候，如果被叫方发出声音（即便还没识别出结果），该话术就结束
    @SerializedName("cancel_on_speak")
    private Boolean cancel_on_speak;

    // 用来表示下发的话术输出的时候，如果被叫方发出声音，AI 就暂停说话，等被叫方说完，AI 再恢复说话
    @SerializedName("pause_on_speak")
    private Boolean pause_on_speak;

    // 用来表示下发的用户中心 - 用户属性
    @SerializedName("user_attrs")
    private Map<String, String> user_attrs;

    // 当前所在的节点ID，与qa_id二选一
    @SerializedName("node_id")
    private Long node_id;

    // 当前所在的问答知识库编号，如B00001。，与node_id二选一
    @SerializedName("qa_id")
    private String qa_id;

    @SerializedName("cps")
    private CompositeVO[] cps;

    @SerializedName("use_esl")
    private Boolean use_esl;
}
