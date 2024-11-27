package com.yulore.medhub.stream;

import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class CompositeVO {
    @SerializedName("type")
    String type;

    @SerializedName("bucket")
    String bucket;

    @SerializedName("object")
    String object;

    @SerializedName("voice")
    String voice;

    @SerializedName("text")
    String text;

    @SerializedName("cache")
    Boolean cache;
}
