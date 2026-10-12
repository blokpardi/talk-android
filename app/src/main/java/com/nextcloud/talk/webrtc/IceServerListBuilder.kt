/*
 * Nextcloud Talk - Android Client
 *
 * SPDX-FileCopyrightText: 2026 Nextcloud GmbH and Nextcloud contributors
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
package com.nextcloud.talk.webrtc

import com.nextcloud.talk.models.json.signaling.settings.IceServerDto
import com.nextcloud.talk.models.json.signaling.settings.SignalingSettingsDto

/**
 * Builds the ICE server list for calls from the per-conversation signaling settings.
 *
 * In a federated conversation the settings of the own server also contain the TURN servers of the host (the host's
 * media server may only be reachable through them), each with its own credentials. All STUN and TURN servers are
 * kept, in server order, with the credentials of the entry they came from; only blank and duplicate URLs are dropped.
 */
object IceServerListBuilder {

    data class IceServerSpec(val url: String, val username: String? = null, val credential: String? = null) {
        val isTurn: Boolean get() = url.startsWith("turn:") || url.startsWith("turns:")
    }

    fun build(settings: SignalingSettingsDto?): List<IceServerSpec> {
        if (settings == null) return emptyList()
        val result = LinkedHashMap<String, IceServerSpec>()
        settings.stunServers?.forEach { server ->
            urlsOf(server).forEach { url -> result.putIfAbsent(url, IceServerSpec(url)) }
        }
        settings.turnServers?.forEach { server ->
            urlsOf(server).forEach { url ->
                result.putIfAbsent(url, IceServerSpec(url, server.username, server.credential))
            }
        }
        return result.values.toList()
    }

    /** URLs for logging, without any credentials. */
    fun describe(servers: List<IceServerSpec>): String =
        if (servers.isEmpty()) "<none>" else servers.joinToString(", ") { it.url }

    private fun urlsOf(server: IceServerDto): List<String> {
        @Suppress("DEPRECATION")
        val urls = server.urls ?: listOfNotNull(server.url)
        return urls.map { it.trim() }.filter { it.isNotEmpty() }
    }
}
