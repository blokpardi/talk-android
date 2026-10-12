/*
 * Nextcloud Talk - Android Client
 *
 * SPDX-FileCopyrightText: 2026 Nextcloud GmbH and Nextcloud contributors
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
package com.nextcloud.talk.webrtc

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class WebSocketReconnectPolicyTest {

    @Test
    fun backoffDoublesFromOneSecondAndCapsAtThirtySeconds() {
        val delays = (0..8).map { WebSocketInstance.reconnectDelayMillis(it, 0.0) }
        assertEquals(listOf(1000L, 2000L, 4000L, 8000L, 16000L, 30000L, 30000L, 30000L, 30000L), delays)
    }

    @Test
    fun jitterAddsAtMostTwentyPercent() {
        assertEquals(1199L, WebSocketInstance.reconnectDelayMillis(0, 0.999))
        assertEquals(36000L, WebSocketInstance.reconnectDelayMillis(10, 1.0))
    }

    @Test
    fun negativeAttemptIsTreatedAsFirstAttempt() {
        assertEquals(1000L, WebSocketInstance.reconnectDelayMillis(-3, 0.0))
    }

    @Test
    fun rejoinsOnlyWithRoomAndValidSession() {
        assertTrue(WebSocketInstance.shouldRejoinRoom("abc123", "session"))
        assertFalse(WebSocketInstance.shouldRejoinRoom(null, "session"))
        assertFalse(WebSocketInstance.shouldRejoinRoom("", "session"))
        assertFalse(WebSocketInstance.shouldRejoinRoom("abc123", null))
        assertFalse(WebSocketInstance.shouldRejoinRoom("abc123", "0"))
    }
}
