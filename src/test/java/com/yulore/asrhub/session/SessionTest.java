package com.yulore.asrhub.session;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
class SessionTest {

    @Test
    void startTranscription() {
        Session session = new Session();

        // return true for the first time
        assertTrue(session.startTranscription());

        // return false for the second time
        assertFalse(session.startTranscription());
    }
}