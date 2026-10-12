/*
 * Nextcloud Talk - Android Client
 *
 * SPDX-FileCopyrightText: 2023 Marcel Hibbe <dev@mhibbe.de>
 * SPDX-FileCopyrightText: 2017-2018 Mario Danic <mario@lovelyhq.com>
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
package com.nextcloud.talk.webrtc

import android.content.Context
import android.text.TextUtils
import android.util.Log
import autodagger.AutoInjector
import com.bluelinelabs.logansquare.LoganSquare
import com.nextcloud.talk.api.NcApi
import com.nextcloud.talk.application.NextcloudTalkApplication
import com.nextcloud.talk.application.NextcloudTalkApplication.Companion.sharedApplication
import com.nextcloud.talk.data.user.model.User
import com.nextcloud.talk.events.NetworkEvent
import com.nextcloud.talk.events.WebSocketCommunicationEvent
import com.nextcloud.talk.models.json.participants.ParticipantDto
import com.nextcloud.talk.models.json.participants.ParticipantDto.ActorType
import com.nextcloud.talk.models.json.signaling.NCSignalingMessageDto
import com.nextcloud.talk.models.json.signaling.settings.FederationSettingsDto
import com.nextcloud.talk.models.json.websocket.BaseWebSocketMessageDto
import com.nextcloud.talk.models.json.websocket.ByeWebSocketMessageDto
import com.nextcloud.talk.models.json.websocket.CallOverallWebSocketMessage
import com.nextcloud.talk.models.json.websocket.CallWebSocketMessageDto
import com.nextcloud.talk.models.json.websocket.ErrorOverallWebSocketMessage
import com.nextcloud.talk.models.json.websocket.EventOverallWebSocketMessage
import com.nextcloud.talk.models.json.websocket.HelloResponseOverallWebSocketMessage
import com.nextcloud.talk.models.json.websocket.JoinedRoomOverallWebSocketMessage
import com.nextcloud.talk.signaling.SignalingMessageReceiver
import com.nextcloud.talk.signaling.SignalingMessageSender
import com.nextcloud.talk.utils.ApiUtils
import com.nextcloud.talk.utils.bundle.BundleKeys
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.ByteString
import org.greenrobot.eventbus.EventBus
import org.greenrobot.eventbus.Subscribe
import org.greenrobot.eventbus.ThreadMode
import java.io.IOException
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import javax.inject.Inject

@AutoInjector(NextcloudTalkApplication::class)
@Suppress("TooManyFunctions")
class WebSocketInstance internal constructor(conversationUser: User, connectionUrl: String, webSocketTicket: String) :
    WebSocketListener() {
    @JvmField
    @Inject
    var okHttpClient: OkHttpClient? = null

    @JvmField
    @Inject
    var eventBus: EventBus? = null

    @JvmField
    @Inject
    var context: Context? = null

    @JvmField
    @Inject
    var ncApi: NcApi? = null
    private val conversationUser: User
    private val webSocketTicket: String
    private var resumeId: String? = null
    var sessionId: String? = null
        private set
    private var hasMCU = false
    private var supportsChatRelay = false
    var isConnected: Boolean
        private set
    private val webSocketConnectionHelper: WebSocketConnectionHelper
    private var internalWebSocket: WebSocket? = null
    private val connectionUrl: String
    private var currentRoomToken: String? = null
    private var currentNormalBackendSession: String? = null
    private var currentFederation: FederationSettingsDto? = null
    private var reconnecting = false

    /** Room the app asked to be in (cleared on leave); used to re-join after a reconnect with a fresh session. */
    private var requestedRoomToken: String? = null
    private var reconnectAttempt = 0
    private var pendingReconnect: ScheduledFuture<*>? = null

    @Volatile
    private var isShutDown = false

    // Federated link recovery (see FederationLinkState)
    private var federatedRejoinAttempts = 0
    private var federatedRejoinPending = false
    private var federatedRejoinGeneration = 0
    private var federatedRejoinFuture: ScheduledFuture<*>? = null
    private val usersHashMap: HashMap<String?, ParticipantDto>
    private var messagesQueue: MutableList<String> = ArrayList()
    private val signalingMessageReceiver = ExternalSignalingMessageReceiver()
    val signalingMessageSender = ExternalSignalingMessageSender()
    private val signalingHttpClient: OkHttpClient by lazy { createSignalingHttpClient(okHttpClient!!) }

    init {
        sharedApplication!!.componentApplication.inject(this)
        this.connectionUrl = connectionUrl
        this.conversationUser = conversationUser
        this.webSocketTicket = webSocketTicket
        webSocketConnectionHelper = WebSocketConnectionHelper()
        usersHashMap = HashMap()
        isConnected = false
        eventBus!!.register(this)
        restartWebSocket()
    }

    private fun sendHello() {
        try {
            if (TextUtils.isEmpty(resumeId)) {
                internalWebSocket!!.send(
                    LoganSquare.serialize(
                        webSocketConnectionHelper
                            .getAssembledHelloModel(conversationUser, webSocketTicket)
                    )
                )
            } else {
                internalWebSocket!!.send(
                    LoganSquare.serialize(
                        webSocketConnectionHelper
                            .getAssembledHelloModelForResume(resumeId)
                    )
                )
            }
        } catch (e: IOException) {
            Log.e(TAG, "Failed to serialize hello model")
        }
    }

    override fun onOpen(webSocket: WebSocket, response: Response) {
        val previousWebSocket = internalWebSocket
        Log.d(
            TAG,
            "Open webSocket ${webSocket.hashCode()} (previous was ${previousWebSocket?.hashCode()})"
        )
        internalWebSocket = webSocket
        sendHello()
    }

    private fun closeWebSocket(webSocket: WebSocket) {
        Log.d(TAG, "closeWebSocket ${webSocket.hashCode()}")
        webSocket.close(NORMAL_CLOSURE, null)
        webSocket.cancel()
        if (webSocket !== internalWebSocket) {
            return
        }
        isConnected = false
        messagesQueue = ArrayList()
        scheduleReconnect()
    }

    /**
     * Reconnects after an exponential backoff with jitter (1s, 2s, 4s ... capped at 30s), instead of blocking the
     * OkHttp thread and retrying every second forever. Only one reconnect is ever pending, so repeated failures or
     * closes cannot stack up parallel sockets.
     */
    @Synchronized
    private fun scheduleReconnect() {
        if (isShutDown) {
            Log.d(TAG, "scheduleReconnect: instance is shut down, not reconnecting")
            return
        }
        if (pendingReconnect?.isDone == false) {
            Log.d(TAG, "scheduleReconnect: reconnect already pending")
            return
        }
        val delay = reconnectDelayMillis(reconnectAttempt, ThreadLocalRandom.current().nextDouble())
        reconnectAttempt++
        reconnecting = true
        Log.d(TAG, "scheduleReconnect: attempt $reconnectAttempt in $delay ms")
        pendingReconnect = reconnectExecutor.schedule({ restartWebSocket() }, delay, TimeUnit.MILLISECONDS)
    }

    /** Stops this instance for good: no further reconnects, no event bus subscription. */
    @Synchronized
    fun shutdown() {
        Log.d(TAG, "shutdown")
        isShutDown = true
        pendingReconnect?.cancel(false)
        pendingReconnect = null
        isConnected = false
        if (eventBus?.isRegistered(this) == true) {
            eventBus?.unregister(this)
        }
        internalWebSocket?.close(NORMAL_CLOSURE, null)
    }

    fun clearResumeId() {
        resumeId = ""
    }

    @Synchronized
    fun restartWebSocket() {
        if (isShutDown) {
            Log.d(TAG, "restartWebSocket: instance is shut down")
            return
        }
        pendingReconnect?.let {
            if (!it.isDone) it.cancel(false)
        }
        pendingReconnect = null
        Log.d(TAG, "restartWebSocket: $connectionUrl")
        val previousWebSocket = internalWebSocket
        isConnected = false
        reconnecting = true
        val request = Request.Builder().url(connectionUrl).build()
        internalWebSocket = signalingHttpClient.newWebSocket(request, this)
        previousWebSocket?.close(NORMAL_CLOSURE, null)
        previousWebSocket?.cancel()
    }

    override fun onMessage(webSocket: WebSocket, text: String) {
        if (webSocket === internalWebSocket) {
            Log.d(TAG, "Receiving : $webSocket $text")
            try {
                val (messageType) = LoganSquare.parse(text, BaseWebSocketMessageDto::class.java)
                if (messageType != null) {
                    when (messageType) {
                        "hello" -> processHelloMessage(webSocket, text)
                        "error" -> processErrorMessage(webSocket, text)
                        "room" -> processJoinedRoomMessage(text)
                        "event" -> processEventMessage(text)
                        "message" -> processMessage(text)
                        "bye" -> {
                            isConnected = false
                            resumeId = ""
                        }

                        else -> {}
                    }
                } else {
                    Log.e(TAG, "Received message with type: null")
                }
            } catch (e: IOException) {
                Log.e(TAG, "Failed to recognize WebSocket message", e)
            }
        }
    }

    @Throws(IOException::class)
    private fun processMessage(text: String) {
        val (_, callWebSocketMessage) = LoganSquare.parse(text, CallOverallWebSocketMessage::class.java)
        if (callWebSocketMessage != null) {
            val ncSignalingMessage = callWebSocketMessage.ncSignalingMessage

            if (ncSignalingMessage != null && callWebSocketMessage.senderWebSocketMessage != null) {
                val resolvedFrom = FederatedSenderNormalizer.resolveFrom(
                    ncSignalingMessage.from,
                    callWebSocketMessage.senderWebSocketMessage!!.sessionId,
                    sessionId
                )
                if (resolvedFrom != ncSignalingMessage.from && !TextUtils.isEmpty(ncSignalingMessage.from)) {
                    Log.d(TAG, "Routing ${ncSignalingMessage.type} from own federated session to own publisher")
                }
                ncSignalingMessage.from = resolvedFrom
            }

            signalingMessageReceiver.processChatMessage(callWebSocketMessage)
        }
    }

    @Throws(IOException::class)
    private fun processEventMessage(text: String) {
        val eventOverallWebSocketMessage = LoganSquare.parse(text, EventOverallWebSocketMessage::class.java)
        if (eventOverallWebSocketMessage.eventMap != null) {
            val target = eventOverallWebSocketMessage.eventMap!!["target"] as String?
            if (target != null) {
                when (target) {
                    Globals.TARGET_ROOM -> {
                        if ("message" == eventOverallWebSocketMessage.eventMap!!["type"]) {
                            processRoomMessageMessage(eventOverallWebSocketMessage, text)
                        } else if ("join" == eventOverallWebSocketMessage.eventMap!!["type"]) {
                            processRoomJoinMessage(eventOverallWebSocketMessage)
                        } else if ("leave" == eventOverallWebSocketMessage.eventMap!!["type"]) {
                            processRoomLeaveMessage(eventOverallWebSocketMessage)
                        } else if ("federation_interrupted" == eventOverallWebSocketMessage.eventMap!!["type"]) {
                            Log.i(TAG, "Link to the signaling server of the conversation host interrupted")
                            setFederationLinkInterrupted(true)
                        } else if ("federation_resumed" == eventOverallWebSocketMessage.eventMap!!["type"]) {
                            Log.i(TAG, "Link to the signaling server of the conversation host restored")
                            resetFederatedRejoin()
                            setFederationLinkInterrupted(false)
                        }
                        signalingMessageReceiver.processChatMessage(eventOverallWebSocketMessage.eventMap)
                    }

                    Globals.TARGET_PARTICIPANTS ->
                        signalingMessageReceiver.processChatMessage(eventOverallWebSocketMessage.eventMap)

                    else ->
                        Log.i(TAG, "Received unknown/ignored event target: $target")
                }
            } else {
                Log.w(TAG, "Received message with event target: null")
            }
        }
    }

    private fun processRoomMessageMessage(eventOverallWebSocketMessage: EventOverallWebSocketMessage, text: String) {
        val messageHashMap = eventOverallWebSocketMessage.eventMap?.get("message") as Map<*, *>?

        if (messageHashMap != null && messageHashMap.containsKey("data")) {
            val dataHashMap = messageHashMap["data"] as Map<*, *>?

            if (dataHashMap != null && dataHashMap.containsKey("chat")) {
                val chatMap = dataHashMap["chat"] as Map<*, *>?
                if (chatMap != null && chatMap.containsKey("refresh") && chatMap["refresh"] as Boolean) {
                    val refreshChatHashMap = HashMap<String, String?>()
                    refreshChatHashMap[BundleKeys.KEY_ROOM_TOKEN] = messageHashMap["roomid"] as String?
                    refreshChatHashMap[BundleKeys.KEY_INTERNAL_USER_ID] = (conversationUser.id!!).toString()
                    eventBus!!.post(WebSocketCommunicationEvent("refreshChat", refreshChatHashMap))
                }

                if (chatMap != null && chatMap.containsKey("comment")) {
                    signalingMessageReceiver.processChatMessage(text)
                }
            } else if (dataHashMap != null && dataHashMap.containsKey("recording")) {
                val recordingMap = dataHashMap["recording"] as Map<*, *>?
                if (recordingMap != null && recordingMap.containsKey("status")) {
                    val status = (recordingMap["status"] as Long?)!!.toInt()
                    Log.d(TAG, "status is $status")
                    val recordingHashMap = HashMap<String, String>()
                    recordingHashMap[BundleKeys.KEY_RECORDING_STATE] = status.toString()
                    eventBus!!.post(WebSocketCommunicationEvent("recordingStatus", recordingHashMap))
                }
            }
        }
    }

    private fun processRoomJoinMessage(eventOverallWebSocketMessage: EventOverallWebSocketMessage) {
        val joinEventList = eventOverallWebSocketMessage.eventMap?.get("join") as List<HashMap<String, Any>>?
        var internalHashMap: HashMap<String, Any>
        var participant: ParticipantDto
        for (i in joinEventList!!.indices) {
            internalHashMap = joinEventList[i]
            val userMap = internalHashMap["user"] as HashMap<String, Any>?
            participant = ParticipantDto()
            val userId = internalHashMap["userid"] as String?
            if (userId != null) {
                participant.actorType = ActorType.USERS
                participant.actorId = userId
            } else {
                participant.actorType = ActorType.GUESTS
                // FIXME seems to be not given by the HPB: participant.setActorId();
            }
            if (userMap != null) {
                // There is no "user" attribute for guest participants.
                participant.displayName = userMap["displayname"] as String?
            }
            usersHashMap[internalHashMap["sessionid"] as String?] = participant
        }
    }

    private fun processRoomLeaveMessage(eventOverallWebSocketMessage: EventOverallWebSocketMessage) {
        val leaveEventList = eventOverallWebSocketMessage.eventMap?.get("leave") as List<String>?
        for (i in leaveEventList!!.indices) {
            usersHashMap.remove(leaveEventList[i])
        }
    }

    fun getUserMap(): HashMap<String?, ParticipantDto> = usersHashMap

    @Throws(IOException::class)
    private fun processJoinedRoomMessage(text: String) {
        val (_, roomWebSocketMessage) = LoganSquare.parse(text, JoinedRoomOverallWebSocketMessage::class.java)
        if (roomWebSocketMessage != null) {
            val alreadyInRoom = roomWebSocketMessage.roomId == currentRoomToken
            currentRoomToken = roomWebSocketMessage.roomId
            if (!currentRoomToken.isNullOrEmpty() && currentRoomToken == requestedRoomToken &&
                (federatedRejoinPending || federatedRejoinAttempts > 0)
            ) {
                Log.d(TAG, "Federated conversation joined again, link recovered")
                resetFederatedRejoin()
                setFederationLinkInterrupted(false)
            }
            if (roomWebSocketMessage.roomPropertiesWebSocketMessage != null && !TextUtils.isEmpty(currentRoomToken)) {
                if (alreadyInRoom) {
                    sendRoomUpdatedEvent()
                } else {
                    sendRoomJoinedEvent()
                }
            }
        }
    }

    @Throws(IOException::class)
    private fun processErrorMessage(webSocket: WebSocket, text: String) {
        Log.e(TAG, "Received error: $text")
        val (_, message) = LoganSquare.parse(text, ErrorOverallWebSocketMessage::class.java)
        if (message != null && isFederatedRoomRequested() && isFederatedLinkError(message.code)) {
            // The signaling server gave up the link of the federated conversation to its host (or rejected the
            // federated join, e.g. "token_expired" after the host restarted): poll and re-join with fresh settings.
            Log.w(TAG, "Federated link failed (${message.code}), scheduling re-join")
            scheduleFederatedRejoin()
            return
        }
        if (message != null) {
            if ("no_such_session" == message.code) {
                Log.d(TAG, "WebSocket " + webSocket.hashCode() + " resumeID " + resumeId + " expired")
                resumeId = ""
                currentRoomToken = ""
                currentNormalBackendSession = ""
                restartWebSocket()
            } else if ("hello_expected" == message.code) {
                restartWebSocket()
            } else if ("no_such_room" == message.code) {
                // The room session is stale (e.g. reaped by the server). Clear the cached join state so a retry
                // actually sends, and let the call UI fetch a fresh room session via the joinRoom API.
                Log.d(TAG, "Joining the room was rejected, the room session needs to be refreshed")
                currentRoomToken = ""
                currentNormalBackendSession = ""
                eventBus!!.post(WebSocketCommunicationEvent("roomJoinFailed", HashMap()))
            }
        }
    }

    @Throws(IOException::class)
    private fun processHelloMessage(webSocket: WebSocket, text: String) {
        isConnected = true
        reconnecting = false
        reconnectAttempt = 0
        val oldResumeId = resumeId
        val (_, helloResponseWebSocketMessage1) = LoganSquare.parse(
            text,
            HelloResponseOverallWebSocketMessage::class.java
        )
        if (helloResponseWebSocketMessage1 != null) {
            resumeId = helloResponseWebSocketMessage1.resumeId
            sessionId = helloResponseWebSocketMessage1.sessionId
            hasMCU = helloResponseWebSocketMessage1.serverHasMCUSupport()

            val features =
                helloResponseWebSocketMessage1.serverHelloResponseFeaturesWebSocketMessage?.features ?: emptyList()
            supportsChatRelay = features.contains("chat-relay")
            if (supportsChatRelay) {
                Log.d(TAG, "chat-relay is supported")
            } else {
                Log.d(TAG, "chat-relay is NOT supported")
            }
        }
        for (i in messagesQueue.indices) {
            webSocket.send(messagesQueue[i])
        }
        messagesQueue = ArrayList()
        val helloHashMap = HashMap<String, String?>()
        val roomToRejoin = requestedRoomToken
        val sessionToRejoin = currentNormalBackendSession
        val federationToRejoin = currentFederation
        if (!TextUtils.isEmpty(oldResumeId)) {
            helloHashMap["oldResumeId"] = oldResumeId
        } else {
            currentRoomToken = ""
            currentNormalBackendSession = ""
        }
        if (!TextUtils.isEmpty(currentRoomToken)) {
            helloHashMap[Globals.ROOM_TOKEN] = currentRoomToken
        }
        eventBus!!.post(WebSocketCommunicationEvent("hello", helloHashMap))

        // A fresh signaling session (resume failed) is no longer in any room. Re-join the room the app is still
        // in with its existing Nextcloud room session, so chat relay, typing and participant updates keep working
        // without the UI having to notice. The join is de-duplicated if the UI re-joins as well.
        if (TextUtils.isEmpty(oldResumeId) && shouldRejoinRoom(roomToRejoin, sessionToRejoin)) {
            Log.d(TAG, "Re-joining room $roomToRejoin after reconnect with a new signaling session")
            joinRoomWithRoomTokenAndSession(roomToRejoin!!, sessionToRejoin, federationToRejoin)
        }
    }

    private fun sendRoomJoinedEvent() {
        val joinRoomHashMap = HashMap<String, String?>()
        joinRoomHashMap[Globals.ROOM_TOKEN] = currentRoomToken
        eventBus!!.post(WebSocketCommunicationEvent("roomJoined", joinRoomHashMap))
    }

    private fun sendRoomUpdatedEvent() {
        val hashMap = HashMap<String, String?>()
        hashMap[Globals.ROOM_TOKEN] = currentRoomToken
        eventBus!!.post(WebSocketCommunicationEvent("roomUpdated", hashMap))
    }

    override fun onMessage(webSocket: WebSocket, bytes: ByteString) {
        Log.d(TAG, "Receiving bytes : " + bytes.hex())
    }

    override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
        Log.d(TAG, "onClosing : WebSocket ${webSocket.hashCode()} $code / $reason")
    }

    override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
        Log.d(TAG, "onClosed : WebSocket ${webSocket.hashCode()} $code / $reason")
        if (webSocket !== internalWebSocket) {
            return
        }
        isConnected = false
        // Closed by the signaling server (restart, idle timeout, bye): reconnect instead of waiting for the next
        // outgoing message or network change to notice.
        scheduleReconnect()
    }

    override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
        val isCurrent = webSocket === internalWebSocket
        Log.e(TAG, "Error : WebSocket ${webSocket.hashCode()} (isCurrentInternalWebSocket=$isCurrent)", t)
        closeWebSocket(webSocket)
    }

    fun hasMCU(): Boolean = hasMCU
    /** Chat relay is withheld while the federated link of the requested room is interrupted, so the chat polls. */
    fun supportsChatRelay(): Boolean = supportsChatRelay && !FederationLinkState.isInterrupted(requestedRoomToken)

    private fun isFederatedRoomRequested(): Boolean =
        !requestedRoomToken.isNullOrEmpty() && !currentFederation?.server.isNullOrEmpty()

    private fun isFederatedLinkError(code: String?): Boolean =
        code == "token_expired" ||
            (FederationLinkState.isInterrupted(requestedRoomToken) && code !in NON_FEDERATION_ERRORS)

    private fun setFederationLinkInterrupted(interrupted: Boolean) {
        val token = requestedRoomToken ?: currentRoomToken
        if (FederationLinkState.setInterrupted(token, interrupted)) {
            val map = HashMap<String, String?>()
            map[Globals.ROOM_TOKEN] = token
            map["interrupted"] = interrupted.toString()
            eventBus?.post(WebSocketCommunicationEvent("federationLinkChanged", map))
        }
    }

    @Synchronized
    private fun scheduleFederatedRejoin() {
        val token = requestedRoomToken
        if (isShutDown || token.isNullOrEmpty() || currentFederation?.server.isNullOrEmpty() ||
            federatedRejoinPending
        ) {
            return
        }
        federatedRejoinPending = true
        setFederationLinkInterrupted(true)
        val generation = ++federatedRejoinGeneration
        val delay = FederationLinkState.rejoinDelayMillis(federatedRejoinAttempts)
        federatedRejoinAttempts++
        Log.d(TAG, "Federated re-join attempt $federatedRejoinAttempts in $delay ms")
        federatedRejoinFuture = reconnectExecutor.schedule(
            { rejoinFederatedRoom(token, generation) },
            delay,
            TimeUnit.MILLISECONDS
        )
    }

    @Synchronized
    private fun resetFederatedRejoin() {
        federatedRejoinFuture?.cancel(false)
        federatedRejoinFuture = null
        federatedRejoinPending = false
        federatedRejoinAttempts = 0
        federatedRejoinGeneration++
    }

    /** Runs on the reconnect thread: fetches fresh signaling settings, then leaves and joins the room again. */
    private fun rejoinFederatedRoom(token: String, generation: Int) {
        fun isOutdated() = generation != federatedRejoinGeneration || token != requestedRoomToken || isShutDown
        if (isOutdated()) return

        val federation = try {
            val apiVersion = ApiUtils.getSignalingApiVersion(conversationUser, intArrayOf(ApiUtils.API_V3, 2, 1))
            ncApi!!.getSignalingSettings(
                ApiUtils.getCredentials(conversationUser.username, conversationUser.token),
                ApiUtils.getUrlForSignalingSettings(apiVersion, conversationUser.baseUrl, token)
            ).blockingFirst().ocs?.settings?.federation ?: FederationSettingsDto()
        } catch (e: Exception) {
            Log.w(TAG, "Failed to get signaling settings of the federated conversation", e)
            null
        }

        synchronized(this) {
            if (isOutdated()) return
            federatedRejoinPending = false
            when (FederationLinkState.decideRejoin(federation != null, federation?.server)) {
                FederationLinkState.RejoinDecision.RETRY -> scheduleFederatedRejoin()
                FederationLinkState.RejoinDecision.STOP ->
                    Log.i(TAG, "Federated conversation has no host signaling server any more, keep polling")
                FederationLinkState.RejoinDecision.REJOIN -> {
                    if (!isConnected) {
                        // A new session re-joins by itself (processHelloMessage); a resumed one by the next attempt
                        scheduleFederatedRejoin()
                        return
                    }
                    val session = currentNormalBackendSession
                    Log.i(TAG, "Joining the federated conversation $token again")
                    try {
                        sendMessage(
                            LoganSquare.serialize(
                                webSocketConnectionHelper.getAssembledJoinOrLeaveRoomModel("", session, null)
                            )
                        )
                    } catch (e: IOException) {
                        Log.e(TAG, "Failed to serialize leave message", e)
                    }
                    currentRoomToken = ""
                    currentFederation = federation
                    joinRoomWithRoomTokenAndSession(token, session, federation)
                }
            }
        }
    }

    @Suppress("Detekt.ComplexMethod")
    fun joinRoomWithRoomTokenAndSession(
        roomToken: String,
        normalBackendSession: String?,
        federation: FederationSettingsDto? = null
    ) {
        Log.d(TAG, "joinRoomWithRoomTokenAndSession")
        Log.d(TAG, "   roomToken: $roomToken")
        Log.d(TAG, "   session: $normalBackendSession")
        try {
            val message = LoganSquare.serialize(
                webSocketConnectionHelper.getAssembledJoinOrLeaveRoomModel(roomToken, normalBackendSession, federation)
            )
            if (roomToken != requestedRoomToken) {
                resetFederatedRejoin()
                FederationLinkState.setInterrupted(requestedRoomToken, false)
            }
            requestedRoomToken = roomToken.ifEmpty { null }
            if (roomToken == "") {
                Log.d(TAG, "sending 'leave room' via websocket")
                currentNormalBackendSession = ""
                currentFederation = null
                sendMessage(message)
            } else if (
                roomToken == currentRoomToken &&
                normalBackendSession == currentNormalBackendSession &&
                federation?.roomId == currentFederation?.roomId &&
                federation?.nextcloudServer == currentFederation?.nextcloudServer
            ) {
                Log.d(TAG, "roomToken & session are unchanged. Joining locally without to send websocket message")
                sendRoomJoinedEvent()
            } else {
                Log.d(TAG, "Sending join room message via websocket")
                currentNormalBackendSession = normalBackendSession
                currentFederation = federation
                sendMessage(message)
            }
        } catch (e: IOException) {
            Log.e(TAG, "Failed to serialize signaling message", e)
        }
    }

    private fun sendCallMessage(ncSignalingMessage: NCSignalingMessageDto) {
        try {
            val message = LoganSquare.serialize(
                webSocketConnectionHelper.getAssembledCallMessageModel(ncSignalingMessage)
            )
            sendMessage(message)
        } catch (e: IOException) {
            Log.e(TAG, "Failed to serialize signaling message", e)
        }
    }

    private fun sendMessage(message: String) {
        if (!isConnected || reconnecting) {
            messagesQueue.add(message)

            if (!reconnecting) {
                restartWebSocket()
            }
        } else {
            if (!internalWebSocket!!.send(message)) {
                messagesQueue.add(message)
                restartWebSocket()
            }
        }
    }

    fun sendBye() {
        if (isConnected) {
            try {
                val byeWebSocketMessage = ByeWebSocketMessageDto()
                byeWebSocketMessage.type = "bye"
                byeWebSocketMessage.bye = HashMap()
                internalWebSocket!!.send(LoganSquare.serialize(byeWebSocketMessage))
            } catch (e: IOException) {
                Log.e(TAG, "Failed to serialize bye message")
            }
        }
    }

    fun getDisplayNameForSession(session: String?): String? {
        val participant = usersHashMap[session]
        if (participant != null) {
            if (participant.displayName != null) {
                return participant.displayName
            }
        }
        return ""
    }

    @Subscribe(threadMode = ThreadMode.BACKGROUND)
    fun onMessageEvent(networkEvent: NetworkEvent) {
        if (networkEvent.networkConnectionEvent == NetworkEvent.NetworkConnectionEvent.NETWORK_CONNECTED &&
            !isConnected
        ) {
            restartWebSocket()
        }
    }

    fun getSignalingMessageReceiver(): SignalingMessageReceiver = signalingMessageReceiver

    /**
     * Temporary implementation of SignalingMessageReceiver until signaling related code is extracted to a Signaling
     * class.
     *
     *
     * All listeners are called in the WebSocket reader thread. This thread should be the same as long as the WebSocket
     * stays connected, but it may change whenever it is connected again.
     */
    private class ExternalSignalingMessageReceiver : SignalingMessageReceiver() {
        fun processChatMessage(eventMap: Map<String, Any>?) {
            processEvent(eventMap)
        }

        fun processChatMessage(message: CallWebSocketMessageDto?) {
            if (message?.ncSignalingMessage?.type == "startedTyping" ||
                message?.ncSignalingMessage?.type == "stoppedTyping"
            ) {
                processCallWebSocketMessage(message)
            } else {
                processSignalingMessage(message?.ncSignalingMessage)
            }
        }

        fun processChatMessage(jsonString: String) {
            processChatMessageWebSocketMessage(jsonString)
            Log.d(TAG, "processing Received chat message")
        }
    }

    inner class ExternalSignalingMessageSender : SignalingMessageSender {
        override fun send(ncSignalingMessage: NCSignalingMessageDto) {
            sendCallMessage(ncSignalingMessage)
        }
    }

    companion object {
        private const val TAG = "WebSocketInstance"
        private const val NORMAL_CLOSURE = 1000
        private const val INITIAL_RECONNECT_DELAY_MS: Long = 1000
        private const val MAX_RECONNECT_DELAY_MS: Long = 30000
        private const val MAX_BACKOFF_SHIFT = 5
        private val NON_FEDERATION_ERRORS = setOf("no_such_session", "hello_expected", "processing_failed")
        private const val JITTER_FRACTION = 0.2

        private val reconnectExecutor = Executors.newSingleThreadScheduledExecutor { runnable ->
            Thread(runnable, "SignalingReconnect").apply { isDaemon = true }
        }

        /** Backoff delay for reconnect [attempt] (0-based), with up to +20% jitter from [random] in [0, 1). */
        internal fun reconnectDelayMillis(attempt: Int, random: Double): Long {
            val base = (INITIAL_RECONNECT_DELAY_MS shl attempt.coerceIn(0, MAX_BACKOFF_SHIFT))
                .coerceAtMost(MAX_RECONNECT_DELAY_MS)
            return base + (base * JITTER_FRACTION * random.coerceIn(0.0, 1.0)).toLong()
        }

        internal fun shouldRejoinRoom(roomToken: String?, normalBackendSession: String?): Boolean =
            !roomToken.isNullOrEmpty() && !normalBackendSession.isNullOrEmpty() && normalBackendSession != "0"
        private const val PING_INTERVAL_SECONDS: Long = 30

        // Dedicated client with pings, so half-open WebSocket connections
        // (e.g. after a WiFi to cellular switch without TCP reset) fail and trigger the reconnect path.
        internal fun createSignalingHttpClient(baseClient: OkHttpClient): OkHttpClient =
            baseClient.newBuilder()
                .pingInterval(PING_INTERVAL_SECONDS, TimeUnit.SECONDS)
                .build()
    }
}
