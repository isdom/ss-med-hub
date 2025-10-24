package com.yulore.nlu;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.yulore.znc.vo.DiscourseMarkerVO;
import com.yulore.znc.vo.RemovePhraseResult;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class NluUtil {
    private static final HashFunction _MD5 = Hashing.md5();

    public static String corpusIdOf(final String corpusText) {
        return String.valueOf(_MD5.hashString(corpusText, StandardCharsets.UTF_8));
    }

    public static RemovePhraseResult extractAndRemoveDM(final Map<Integer, DiscourseMarkerVO> dms, final String text) {
        final AtomicReference<String> ref = new AtomicReference<>(text);
        final Set<Integer> ids = new HashSet<>();
        dms.values().forEach(dm ->
            dm.getPatterns().forEach(ptn -> {
                final var cleaned = DMExtractor.removeDMPhrases(ptn, ref.get());
                if (!Objects.equals(cleaned, ref.get())) {
                    ref.set(cleaned);
                    ids.add(dm.getIdx());
                }
            }));
        if (ids.isEmpty()) {
            return null;
        }
        return new RemovePhraseResult(
                corpusIdOf(text),
                ref.get(),
                corpusIdOf(ref.get()),
                ids.toArray(Integer[]::new));
    }

    public static RemovePhraseResult extractAndRemoveClause(final Map<String, Integer> cu2in, final String text) {
        if (cu2in == null || cu2in.isEmpty()) {
            // empty cu2in
            return null;
        }
        final var cus = ClauseSplitter.splitClausesWithPunctuation(text);
        final AtomicReference<String> ref = new AtomicReference<>(text);
        final Set<Integer> ids = new HashSet<>();
        cus.forEach(cu -> {
            final var iid = cu2in.get(ClauseSplitter.erasePunctuation(cu));
            if (iid != null) {
                ids.add(iid);
                ref.set(ref.get().replace(cu, ""));
            }
        });
        if (ids.isEmpty()) {
            return null;
        }
        return new RemovePhraseResult(
                corpusIdOf(text),
                ref.get(),
                corpusIdOf(ref.get()),
                ids.toArray(Integer[]::new));
    }

    public static RemovePhraseResult extractAndRemoveDefinedPhrase(
            final Map<Integer, DiscourseMarkerVO> dms,
            final Map<String, Integer> cu2in,
            final String text) {
        final var dmr = extractAndRemoveDM(dms, text);
        if (dmr != null) {
            if (dmr.cleanedText().isEmpty()) {
                // equals.(""), DM(s) has remove text's all Phrase
                return dmr;
            } else {
                // need merge two result's ins
                final var cur = extractAndRemoveClause(cu2in, dmr.cleanedText());
                if (cur == null) {
                    // not change anything
                    return dmr;
                } else {
                    final Set<Integer> ins = new HashSet<>(Arrays.asList(dmr.ins()));
                    ins.addAll(Arrays.asList(cur.ins()));

                    // Both remove sth. from text
                    return new RemovePhraseResult(
                            dmr.orgId(),
                            cur.cleanedText(),
                            cur.cleanedId(),
                            ins.toArray(Integer[]::new));
                }
            }
        } else {
            return extractAndRemoveClause(cu2in, text);
        }
    }
}
