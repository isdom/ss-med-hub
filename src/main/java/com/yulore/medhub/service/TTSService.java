package com.yulore.medhub.service;

import com.yulore.medhub.nls.CosyAgent;
import com.yulore.medhub.nls.TTSAgent;

import java.util.concurrent.CompletionStage;

public interface TTSService {
    CompletionStage<TTSAgent> selectTTSAgentAsync();
    CompletionStage<CosyAgent> selectCosyAgentAsync();
}
