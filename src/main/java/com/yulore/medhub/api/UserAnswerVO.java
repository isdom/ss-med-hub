package com.yulore.medhub.api;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode(callSuper = true)
@Data
@ToString(callSuper = true)
public class UserAnswerVO extends AIReplyVO {
    private AiSettingVO aiSetting;
}
