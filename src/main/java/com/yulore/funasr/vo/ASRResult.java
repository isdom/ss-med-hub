package com.yulore.funasr.vo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.ToString;

@Data
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class ASRResult {
    @Data
    @ToString
    @JsonIgnoreProperties(ignoreUnknown = true)
    static public class Sentence {
        public String text_seg;
        public long start;
        public long end;
        public String punc;
        public int[][] ts_list;
    }

    public String mode;
    public String wav_name;
    public String text;
    public boolean is_final;
    public String timestamp;
    public Sentence[] stamp_sents;
}
