/*
 * Nextcloud Talk - Android Client
 *
 * SPDX-FileCopyrightText: 2026 Nextcloud GmbH and Nextcloud contributors
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
package com.nextcloud.talk.webrtc

import com.nextcloud.talk.models.json.signaling.settings.IceServerDto
import com.nextcloud.talk.models.json.signaling.settings.SignalingSettingsDto
import com.nextcloud.talk.webrtc.IceServerListBuilder.IceServerSpec
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class IceServerListBuilderTest {

    @Test
    fun federatedSettingsKeepHostAndOwnTurnServersWithTheirOwnCredentials() {
        val settings = SignalingSettingsDto(
            stunServers = listOf(IceServerDto(urls = listOf("stun:cloud.example:3478"))),
            turnServers = listOf(
                IceServerDto(
                    urls = listOf("turn:host.example:3478?transport=udp", "turn:host.example:3478?transport=tcp"),
                    username = "hostUser",
                    credential = "hostCred"
                ),
                IceServerDto(
                    urls = listOf("turn:cloud.example:3478?transport=udp", "turns:cloud.example:443?transport=tcp"),
                    username = "ownUser",
                    credential = "ownCred"
                )
            )
        )

        val result = IceServerListBuilder.build(settings)

        assertEquals(
            listOf(
                IceServerSpec("stun:cloud.example:3478"),
                IceServerSpec("turn:host.example:3478?transport=udp", "hostUser", "hostCred"),
                IceServerSpec("turn:host.example:3478?transport=tcp", "hostUser", "hostCred"),
                IceServerSpec("turn:cloud.example:3478?transport=udp", "ownUser", "ownCred"),
                IceServerSpec("turns:cloud.example:443?transport=tcp", "ownUser", "ownCred")
            ),
            result
        )
        assertEquals(4, result.count { it.isTurn })
    }

    @Suppress("DEPRECATION")
    @Test
    fun legacyUrlFieldBlankAndDuplicateUrlsAreHandled() {
        val settings = SignalingSettingsDto(
            stunServers = listOf(IceServerDto(url = "stun:a:3478"), IceServerDto(urls = listOf(" ", "stun:a:3478"))),
            turnServers = listOf(
                IceServerDto(urls = listOf("turn:t:3478"), username = "u1", credential = "c1"),
                IceServerDto(urls = listOf("turn:t:3478"), username = "u2", credential = "c2")
            )
        )
        assertEquals(
            listOf(IceServerSpec("stun:a:3478"), IceServerSpec("turn:t:3478", "u1", "c1")),
            IceServerListBuilder.build(settings)
        )
    }

    @Test
    fun describeNeverContainsCredentials() {
        val text = IceServerListBuilder.describe(listOf(IceServerSpec("turn:t:3478", "secretUser", "secretCred")))
        assertEquals("turn:t:3478", text)
        assertTrue(IceServerListBuilder.build(null).isEmpty())
        assertEquals("<none>", IceServerListBuilder.describe(emptyList()))
    }
}
