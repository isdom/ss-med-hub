package com.yulore.funasr.vo;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

@Data
@Builder
@ToString
public class StartASR {
    public String mode;
    public String wav_name;
    public boolean is_speaking;
    public String wav_format;
    public int audio_fs;
    public int[] chunk_size;
    public boolean itn;
}
