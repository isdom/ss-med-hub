package com.yulore.medhub.stream;

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

    static public int extractValueAsInteger(final String vars, final String name, int defaultValue) {
        final String value = extractValue(vars, name);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ex) {
            return defaultValue;
        }
    }
}
