package com.yulore.medhub.api;

import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class CompositeVO {
    private String type;

    private String bucket;

    private String object;

    private String voice;

    private String text;

    private Boolean cache;

    private int volume = 50;

    private int speech_rate = 0;

    private int pitch_rate = 0;
}
