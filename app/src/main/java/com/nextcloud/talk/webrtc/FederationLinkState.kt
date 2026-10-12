/*
 * Nextcloud Talk - Android Client
 *
 * SPDX-FileCopyrightText: 2026 Nextcloud GmbH and Nextcloud contributors
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
package com.nextcloud.talk.webrtc

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update

/**
 * Tracks federated conversations whose link from the signaling server to the signaling server of their host is
 * interrupted ("federation_interrupted" until "federation_resumed" or a successful re-join). While a room is in here,
 * chat relay can't be trusted and the chat polls for new messages instead.
 */
object FederationLinkState {
    private val _interruptedRooms = MutableStateFlow<Set<String>>(emptySet())
    val interruptedRooms: StateFlow<Set<String>> = _interruptedRooms

    /** Delays of consecutive federated re-join attempts, like the web client (0s, 5s, 10s, 20s, 40s, then 60s). */
    private val REJOIN_DELAYS_MS = longArrayOf(0, 5_000, 10_000, 20_000, 40_000, 60_000)

    fun isInterrupted(roomToken: String?): Boolean =
        !roomToken.isNullOrEmpty() && _interruptedRooms.value.contains(roomToken)

    /** @return true when the state changed */
    fun setInterrupted(roomToken: String?, interrupted: Boolean): Boolean {
        if (roomToken.isNullOrEmpty()) {
            return false
        }
        var changed = false
        _interruptedRooms.update { rooms ->
            val next = if (interrupted) rooms + roomToken else rooms - roomToken
            changed = next != rooms
            next
        }
        return changed
    }

    fun clear() {
        _interruptedRooms.value = emptySet()
    }

    fun rejoinDelayMillis(attempt: Int): Long = REJOIN_DELAYS_MS[attempt.coerceIn(0, REJOIN_DELAYS_MS.size - 1)]

    enum class RejoinDecision { RETRY, STOP, REJOIN }

    /**
     * What to do with the result of fetching fresh signaling settings for a federated re-join: a failed request (e.g.
     * the HTTP 500 the server answers with while the host is unreachable) retries after the backoff, settings without
     * a federation block mean there is nothing to re-join (keep polling), otherwise re-join with the new settings.
     */
    fun decideRejoin(fetchSucceeded: Boolean, federationServer: String?): RejoinDecision =
        when {
            !fetchSucceeded -> RejoinDecision.RETRY
            federationServer.isNullOrEmpty() -> RejoinDecision.STOP
            else -> RejoinDecision.REJOIN
        }
}
