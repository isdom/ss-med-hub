package com.yulore.medhub.api;

import lombok.Data;

@Data
public class AiSettingVO {
    private String asr_provider;
    private int idle_timeout;
    private String answer_record_file;
}