/*
 * Nextcloud Talk - Android Client
 *
 * SPDX-FileCopyrightText: 2026 Nextcloud GmbH and Nextcloud contributors
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
package com.nextcloud.talk.webrtc

/**
 * Works out the "from" session used to route an incoming WebRTC signaling message to its peer connection.
 *
 * In a federated conversation, the trickled ICE candidates of the own MCU publisher come back with the session id
 * of the user on the host server in "from" and "to" (the signaling federation proxy doesn't rewrite them), while
 * the sender is the own local session. Routing them by "from" drops them: the publisher never gets the candidates
 * of the host's media server and stays in CHECKING (the host's Janus then reports "ICE failed, trickle pending").
 * A message whose sender is the own session always belongs to an own (publisher) connection, so it's routed there.
 */
object FederatedSenderNormalizer {

    fun resolveFrom(from: String?, senderSessionId: String?, ownSessionId: String?): String? {
        if (from.isNullOrEmpty()) return senderSessionId
        if (!ownSessionId.isNullOrEmpty() && senderSessionId == ownSessionId && from != ownSessionId) {
            return ownSessionId
        }
        return from
    }
}
