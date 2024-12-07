package com.yulore.medhub.api;

import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class CompositeVO {
    @SerializedName("type")
    private String type;

    @SerializedName("bucket")
    private String bucket;

    @SerializedName("object")
    private String object;

    @SerializedName("voice")
    private String voice;

    @SerializedName("text")
    private String text;

    @SerializedName("cache")
    private Boolean cache;
}
