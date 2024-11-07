package com.yulore.medhub.task;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@AllArgsConstructor
@Data
@ToString
public class SampleInfo {
    final int sampleRate;
    final int interval;
    final int sampleSizeInBits;
    final int channels;

    public int lenInBytes() {
        return (sampleRate / (1000 / interval) * (sampleSizeInBits / 8)) * channels;
    }

    public int sampleSizeInBytes() {
        return sampleSizeInBits / 8;
    }
}
