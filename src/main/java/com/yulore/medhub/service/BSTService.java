package com.yulore.medhub.service;

import com.yulore.bst.BuildStreamTask;

public interface BSTService {
    BuildStreamTask getTaskOf(final String path, final boolean removeWavHdr, final int sampleRate);
}
