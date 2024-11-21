package com.yulore.medhub.session;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
class MediaSessionTest {

    @Test
    void startTranscription() {
        final MediaSession session = new MediaSession("test", false, 0, false, 0.0f, ()->{});

        // return true for the first time
        assertTrue(session.startTranscription());

        // return false for the second time
        assertFalse(session.startTranscription());
    }
}