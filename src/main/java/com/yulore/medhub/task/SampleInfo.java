package com.yulore.medhub.task;

import lombok.AllArgsConstructor;
import lombok.ToString;

@AllArgsConstructor
@ToString
public class SampleInfo {
    final int sampleRate;
    final int interval;
    final int sampleSizeInBits;
    final int channels;

    public int lenInBytes() {
        return (sampleRate / (1000 / interval) * (sampleSizeInBits / 8)) * channels;
    }
}
