package com.yulore.medhub.task;

public record SampleInfo(int sampleRate, int interval, int sampleSizeInBits, int channels) {
    public int bytesPerInterval() {
        return (sampleRate / (1000 / interval) * (sampleSizeInBits / 8)) * channels;
    }

    public int sampleSizeInBytes() {
        return sampleSizeInBits / 8;
    }
}
