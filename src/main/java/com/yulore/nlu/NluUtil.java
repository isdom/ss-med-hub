package com.yulore.nlu;

import com.yulore.znc.util.RedisUtil;
import com.yulore.znc.vo.DiscourseMarkerVO;
import com.yulore.znc.vo.RemoveDMResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class NluUtil {
    public static RemoveDMResult extractAndRemoveDM(final Map<Integer, DiscourseMarkerVO> dms, final String text) {
        final AtomicReference<String> ref = new AtomicReference<>(text);
        final List<Integer> ids = new ArrayList<>();
        dms.values().forEach(dm -> {
            final var cleaned = DMExtractor.removeDMPhrases(dm.getPattern(), ref.get());
            if (!Objects.equals(cleaned, ref.get())) {
                ref.set(cleaned);
                ids.add(dm.getIdx());
            }
        });
        return new RemoveDMResult(ref.get(), RedisUtil.corpusIdOf(ref.get()), ids.toArray(Integer[]::new));
    }
}
