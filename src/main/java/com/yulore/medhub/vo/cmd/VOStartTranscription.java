package com.yulore.medhub.vo.cmd;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.yulore.medhub.vo.WSCommandVO;
import lombok.Data;
import lombok.ToString;

@Data
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class VOStartTranscription {
    private static final TypeReference<WSCommandVO<VOStartTranscription>> TYPE = new TypeReference<>() {};
    public static VOStartTranscription of(final String message) throws JsonProcessingException {
        return WSCommandVO.parse(message, TYPE).payload;
    }

    public String provider;

    // tx
    public String noise_threshold;
    public String engine_model_type;
    public String input_sample_rate;
    public String vad_silence_time;

    // ali
    public String speech_noise_threshold;
}
