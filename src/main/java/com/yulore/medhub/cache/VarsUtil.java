package com.yulore.medhub.cache;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VarsUtil {
    static public String extractValue(final String vars, final String name) {
        final String prefix = name + "=";
        int varBeginIdx = vars.indexOf(prefix);
        if (varBeginIdx == -1) {
            log.info("{} missing {} field, ignore", vars, name);
            return null;
        }

        int varEndIdx = vars.indexOf(',', varBeginIdx);

        return vars.substring(varBeginIdx + prefix.length(), varEndIdx == -1 ? vars.length() : varEndIdx);
    }
}
