/*
 * Nextcloud Talk - Android Client
 *
 * SPDX-FileCopyrightText: 2026 Nextcloud GmbH and Nextcloud contributors
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
package com.nextcloud.talk.webrtc

import org.junit.Assert.assertEquals
import org.junit.Test

class FederatedSenderNormalizerTest {
    private val own = "localOwn"

    @Test
    fun publisherCandidateWithHostSessionIdIsRoutedToOwnSession() {
        assertEquals(own, FederatedSenderNormalizer.resolveFrom("remoteOwnOnHost", own, own))
    }

    @Test
    fun messagesFromOtherParticipantsAreUnchanged() {
        assertEquals("paul", FederatedSenderNormalizer.resolveFrom("paul", "paul", own))
        assertEquals(own, FederatedSenderNormalizer.resolveFrom(own, own, own))
    }

    @Test
    fun missingFromFallsBackToSender() {
        assertEquals("paul", FederatedSenderNormalizer.resolveFrom(null, "paul", own))
        assertEquals("paul", FederatedSenderNormalizer.resolveFrom("", "paul", own))
    }

    @Test
    fun unknownOwnSessionKeepsFrom() {
        assertEquals("x", FederatedSenderNormalizer.resolveFrom("x", "y", null))
    }
}
