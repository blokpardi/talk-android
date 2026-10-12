/*
 * Nextcloud Talk - Android Client
 *
 * SPDX-FileCopyrightText: 2026 Nextcloud GmbH and Nextcloud contributors
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
package com.nextcloud.talk.webrtc

import com.nextcloud.talk.webrtc.FederationLinkState.RejoinDecision
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class FederationLinkStateTest {

    @After
    fun tearDown() {
        FederationLinkState.clear()
    }

    @Test
    fun rejoinDelaysGrowLikeTheWebClientAndStayAtOneMinute() {
        val delays = (0..7).map { FederationLinkState.rejoinDelayMillis(it) }
        assertEquals(listOf(0L, 5000L, 10000L, 20000L, 40000L, 60000L, 60000L, 60000L), delays)
        assertEquals(0L, FederationLinkState.rejoinDelayMillis(-1))
    }

    @Test
    fun interruptedStateIsPerRoomAndReportsChanges() {
        assertTrue(FederationLinkState.setInterrupted("room1", true))
        assertFalse(FederationLinkState.setInterrupted("room1", true))
        assertTrue(FederationLinkState.isInterrupted("room1"))
        assertFalse(FederationLinkState.isInterrupted("room2"))
        assertTrue(FederationLinkState.setInterrupted("room1", false))
        assertFalse(FederationLinkState.isInterrupted("room1"))
    }

    @Test
    fun emptyTokensAreIgnored() {
        assertFalse(FederationLinkState.setInterrupted(null, true))
        assertFalse(FederationLinkState.setInterrupted("", true))
        assertFalse(FederationLinkState.isInterrupted(null))
        assertTrue(FederationLinkState.interruptedRooms.value.isEmpty())
    }

    @Test
    fun rejoinDecisionFollowsTheServerContract() {
        // HTTP 500 while the host is unreachable -> retry with backoff
        assertEquals(RejoinDecision.RETRY, FederationLinkState.decideRejoin(false, null))
        // 200 without a federation block -> nothing to re-join, keep polling
        assertEquals(RejoinDecision.STOP, FederationLinkState.decideRejoin(true, null))
        assertEquals(RejoinDecision.STOP, FederationLinkState.decideRejoin(true, ""))
        assertEquals(RejoinDecision.REJOIN, FederationLinkState.decideRejoin(true, "wss://host/standalone-signaling"))
    }
}
