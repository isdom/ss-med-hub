package com.yulore.medhub.api;

import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class CompositeVO {
    public String type;

    public String bucket;

    public String object;

    public String voice;

    public String text;

    public Boolean cache;

    public int volume = 50;

    public int speech_rate = 0;

    public int pitch_rate = 0;

    public int start = 0;

    public int end = -1;
}
