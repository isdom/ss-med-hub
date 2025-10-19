package com.yulore.znc.vo;

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;

import java.util.Collection;
import java.util.regex.Pattern;

@Builder
@Data
public class DiscourseMarkerVO {
    private int     idx;
    private String  name;
    private String  desc;
    private String  regex;

    @FieldSerializer.Optional("runtime")
    @ToString.Exclude
    private Collection<Pattern> patterns;

    @Override
    public String toString() {
        return "DM{" +
                "idx=" + idx +
                ", name='" + name + '\'' +
                ", desc='" + desc + '\'' +
                ", regex='" + regex.replace('\r', ' ').replace('\n', ' ') +
                '}';
    }
}
