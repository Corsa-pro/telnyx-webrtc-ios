import Foundation
import WebRTC

/// Decides whether a failed active call restarts ICE or uses reconnect/reattach.
///
/// The monitor owns recovery state and timers, while `Call` owns WebRTC
/// renegotiation and `TxClient` owns reconnect/reattach mechanics.
internal final class SignalingHealthMonitor {
    /// Serializes recovery transitions and timer callbacks independently of the
    /// main queue, where CallKit and other UI-facing work may run.
    private let queue = DispatchQueue(label: "com.telnyx.SignalingHealthMonitor")

    private enum RecoveryMode: Equatable {
        case idle
        case probing
        case iceRestarting
        case verifyingMedia
        case reattaching
        /// The network is gone. Nothing can be done until a path returns;
        /// the health check stays quiet so it does not replace a peer that
        /// may resume on the next path.
        case awaitingPath
        /// A path is back. The peer is kept and the client is replacing the
        /// signaling socket with the same session; media may heal by itself
        /// in the meantime, which `recordInboundRtpPackets` notices.
        case awaitingSignaling
        /// A path changed but the socket is still viable, so nothing was
        /// replaced: libwebrtc is expected to move the media by itself.
        /// Inbound RTP resuming ends it; silence escalates to an ICE restart.
        case watchingMedia
    }

    private enum SignalingProbePurpose {
        case mediaRecovery
        case healthCheck
    }

    private let iceRestartTimeout: TimeInterval
    private let signalingProbeTimeout: TimeInterval
    private let recentInboundActivityThreshold: TimeInterval
    private let confirmedOutboundActivityThreshold: TimeInterval
    private let staleInboundActivityThreshold: TimeInterval
    private let signalingHealthCheckInterval: TimeInterval
    private let postIceRestartMediaTimeout: TimeInterval
    private let peerDisconnectedRecoveryDelay: TimeInterval
    private let pathRecoveryTimeout: TimeInterval
    private let mediaWatchTimeout: TimeInterval
    private let isSignalingAvailable: () -> Bool
    private let sendSignalingProbe: () -> String?
    private let startIceRestart: (Call) -> Void
    private let shouldForceRelayForRecovery: (Call, @escaping (Bool) -> Void) -> Void
    private let requestReattach: (Bool) -> Void
    private let didRecoverInPlace: (Call) -> Void

    private weak var recoveringCall: Call?
    private var recoveryMode: RecoveryMode = .idle
    private var iceRestartTimeoutWorkItem: DispatchWorkItem?
    private var signalingProbeTimeoutWorkItem: DispatchWorkItem?
    private var signalingHealthCheckTimer: DispatchSourceTimer?
    private var mediaVerificationTimeoutWorkItem: DispatchWorkItem?
    private var peerDisconnectedRecoveryWorkItem: DispatchWorkItem?
    private var pathRecoveryTimeoutWorkItem: DispatchWorkItem?
    private var mediaWatchTimeoutWorkItem: DispatchWorkItem?
    private var pendingSignalingProbeId: String?
    private var pendingSignalingProbePurpose: SignalingProbePurpose?
    private var lastInboundSignalingActivity = Date()
    private var lastConfirmedOutboundActivity = Date()
    private weak var monitoredActiveCall: Call?
    private var shouldEvaluateRelayFallback = false
    private var lastInboundRtpPacketCount: Int?
    private var lastInboundRtpProgressAt: Date?
    private var iceRestartStartedAt: Date?
    private var mediaVerificationStartedAt: Date?
    private var pathChangedAt: Date?
    private var mediaHealedAt: Date?

    /// The timeouts are short on purpose. Each of them ends in the reattach,
    /// which is the slow path (3-28 s to audio measured on device), so every
    /// second spent waiting on a step that is not going to succeed is a
    /// second added to the silence. An ICE restart that will work answers
    /// within two or three seconds.
    init(
        iceRestartTimeout: TimeInterval = 6,
        signalingProbeTimeout: TimeInterval = 3,
        recentInboundActivityThreshold: TimeInterval = 3,
        confirmedOutboundActivityThreshold: TimeInterval = 45,
        staleInboundActivityThreshold: TimeInterval = 20,
        signalingHealthCheckInterval: TimeInterval = 3,
        postIceRestartMediaTimeout: TimeInterval = 3,
        peerDisconnectedRecoveryDelay: TimeInterval = 2,
        pathRecoveryTimeout: TimeInterval = 8,
        mediaWatchTimeout: TimeInterval = 3,
        isSignalingAvailable: @escaping () -> Bool,
        sendSignalingProbe: @escaping () -> String?,
        startIceRestart: @escaping (Call) -> Void,
        shouldForceRelayForRecovery: @escaping (Call, @escaping (Bool) -> Void) -> Void = { _, completion in completion(false) },
        requestReattach: @escaping (Bool) -> Void,
        didRecoverInPlace: @escaping (Call) -> Void = { _ in }
    ) {
        self.iceRestartTimeout = iceRestartTimeout
        self.signalingProbeTimeout = signalingProbeTimeout
        self.recentInboundActivityThreshold = recentInboundActivityThreshold
        self.confirmedOutboundActivityThreshold = confirmedOutboundActivityThreshold
        self.staleInboundActivityThreshold = staleInboundActivityThreshold
        self.signalingHealthCheckInterval = signalingHealthCheckInterval
        self.postIceRestartMediaTimeout = postIceRestartMediaTimeout
        self.peerDisconnectedRecoveryDelay = peerDisconnectedRecoveryDelay
        self.pathRecoveryTimeout = pathRecoveryTimeout
        self.mediaWatchTimeout = mediaWatchTimeout
        self.isSignalingAvailable = isSignalingAvailable
        self.sendSignalingProbe = sendSignalingProbe
        self.startIceRestart = startIceRestart
        self.shouldForceRelayForRecovery = shouldForceRelayForRecovery
        self.requestReattach = requestReattach
        self.didRecoverInPlace = didRecoverInPlace
    }

    deinit {
        signalingHealthCheckTimer?.setEventHandler {}
        signalingHealthCheckTimer?.cancel()
        peerDisconnectedRecoveryWorkItem?.cancel()
        pathRecoveryTimeoutWorkItem?.cancel()
        mediaWatchTimeoutWorkItem?.cancel()
    }

    /// The network went away under a live call. The peer is left alone: it
    /// may well carry on once a path returns, and the health check would
    /// otherwise see the dead socket and replace it.
    func networkDidDrop(for call: Call) {
        executeOnQueue { [weak self] in
            guard let self = self, self.recoveryMode != .reattaching else { return }
            Logger.log.w(message: "[CALL-RECOVERY] Network lost; keeping the peer until a path returns")
            self.cancelRecoveryTimeouts()
            self.cancelPeerDisconnectedRecovery()
            self.recoveringCall = call
            self.shouldEvaluateRelayFallback = false
            self.recoveryMode = .awaitingPath
            self.pathChangedAt = nil
            self.mediaHealedAt = nil
            self.lastInboundRtpPacketCount = nil
            call.setCallReportMediaVerificationSamplingEnabled(true)
        }
    }

    /// A path changed under a live call. The peer is kept — libwebrtc gathers
    /// on the new interface and the server latches onto whichever path our
    /// checks arrive from — while the client replaces the signaling socket
    /// with the same session and then reports `signalingDidReconnect`.
    /// Tearing the peer down here, the former flow, cost 3-28 s of silence
    /// on every Wi-Fi/cellular move; the reattach is now the last resort,
    /// reached through `pathRecoveryTimeout` or a failed ICE restart.
    func networkPathDidChange(for call: Call) {
        executeOnQueue { [weak self] in
            guard let self = self, self.recoveryMode != .reattaching else { return }
            Logger.log.i(message: "[CALL-RECOVERY] Network path changed; keeping the peer, replacing the socket")
            self.cancelRecoveryTimeouts()
            self.cancelPeerDisconnectedRecovery()
            self.recoveringCall = call
            self.shouldEvaluateRelayFallback = false
            self.recoveryMode = .awaitingSignaling
            self.pathChangedAt = Date()
            self.mediaHealedAt = nil
            // Packets counted on the old path must not pass for recovery.
            self.lastInboundRtpPacketCount = nil
            self.lastInboundRtpProgressAt = nil
            call.setCallReportMediaVerificationSamplingEnabled(true)
            self.startPathRecoveryTimeout(for: call)
        }
    }

    /// A path changed under a live call while the signaling socket is still
    /// viable (cellular -> Wi-Fi: the cellular socket lives on). Nothing is
    /// replaced. A re-login is what makes the server attach, and an attach
    /// is a rebuilt peer, so the cheapest handover is to not log in again:
    /// libwebrtc gathers on the new interface and moves to it on its own,
    /// the lower-cost network winning, and the server latches onto the
    /// nominated pair. Measured 2026-10-29: every socket swap ended in an
    /// attach, 3.8-4.6 s to audio; this path is meant to be under a second.
    func networkPathDidChangeKeepingSignaling(for call: Call) {
        executeOnQueue { [weak self] in
            guard let self = self, self.recoveryMode == .idle || self.recoveryMode == .watchingMedia else {
                Logger.log.i(message: "[CALL-RECOVERY] Network path changed during a recovery; the recovery stands")
                return
            }
            Logger.log.i(message: "[CALL-RECOVERY] Network path changed with the socket viable; keeping peer and socket, watching media")
            self.cancelPeerDisconnectedRecovery()
            self.recoveringCall = call
            self.shouldEvaluateRelayFallback = false
            self.recoveryMode = .watchingMedia
            self.pathChangedAt = Date()
            self.mediaHealedAt = nil
            self.lastInboundRtpPacketCount = nil
            self.lastInboundRtpProgressAt = nil
            call.setCallReportMediaVerificationSamplingEnabled(true)
            self.startMediaWatchTimeout(for: call)
        }
    }

    /// The server attached the call on its own after a re-login, replacing
    /// the kept peer: that is the reattach done for us, and the attached
    /// call's ACTIVE ends the recovery. Without this the recovery hung on a
    /// call that no longer existed, and the monitor was blind to anything
    /// that happened to the new one.
    func serverDidAttach() {
        executeOnQueue { [weak self] in
            guard let self = self, self.recoveryMode != .idle else { return }
            Logger.log.i(message: "[CALL-RECOVERY] Server attached the call during recovery; waiting for it to be active")
            self.cancelRecoveryTimeouts()
            self.cancelPeerDisconnectedRecovery()
            self.recoveringCall = nil
            self.shouldEvaluateRelayFallback = false
            self.recoveryMode = .reattaching
        }
    }

    /// The replacement socket is registered. Media that healed on its own
    /// ends the recovery; otherwise ICE restarts at once over the new
    /// socket, rather than waiting to see whether it might.
    func signalingDidReconnect() {
        executeOnQueue { [weak self] in
            guard let self = self, self.recoveryMode == .awaitingSignaling, let call = self.recoveringCall else { return }
            self.cancelPathRecoveryTimeout()
            if let healedAt = self.mediaHealedAt, let changedAt = self.pathChangedAt {
                Logger.log.i(message: "[CALL-RECOVERY] Signaling back; inbound RTP resumed \(String(format: "%.1f", healedAt.timeIntervalSince(changedAt)))s after the path change")
                self.finishRecovery(recoveredInPlace: call)
                return
            }
            Logger.log.w(message: "[CALL-RECOVERY] Signaling back; no inbound RTP since the path change, restarting ICE")
            self.startIceRestartRecovery(for: call)
        }
    }

    func iceConnectionStateDidChange(_ call: Call, state: RTCIceConnectionState) {
        guard state == .failed else { return }
        executeOnQueue { [weak self] in
            self?.requestRecovery(for: call, trigger: "ice_failed")
        }
    }

    func peerConnectionStateDidChange(_ call: Call, state: RTCPeerConnectionState) {
        executeOnQueue { [weak self] in
            guard let self = self else { return }
            switch state {
            case .failed:
                self.cancelPeerDisconnectedRecovery()
                self.requestRecovery(for: call, trigger: "peer_connection_failed")
            case .disconnected:
                self.startPeerDisconnectedRecoveryDelay(for: call)
            case .connected:
                self.cancelPeerDisconnectedRecovery()
            default:
                break
            }
        }
    }

    /// Records every inbound signaling frame. A probe is successful only when a
    /// result or error has the exact JSON-RPC id generated for that probe.
    func signalingMessageReceived(_ message: Message) {
        executeOnQueue { [weak self] in
            guard let self = self else { return }
            self.lastInboundSignalingActivity = Date()

            guard message.result != nil || message.serverError != nil else { return }
            self.lastConfirmedOutboundActivity = Date()
            guard self.recoveryMode == .probing,
                  self.pendingSignalingProbeId == message.id,
                  let purpose = self.pendingSignalingProbePurpose else { return }

            Logger.log.i(message: "[CALL-RECOVERY] Signaling health probe succeeded")
            self.cancelSignalingProbeTimeout()
            self.recoveryMode = .idle
            switch purpose {
            case .mediaRecovery:
                guard let call = self.recoveringCall else { return }
                self.startIceRestartRecovery(for: call)
            case .healthCheck:
                self.recoveringCall = nil
            }
        }
    }

    func iceRestartDidComplete(for call: Call) {
        executeOnQueue { [weak self] in
            guard let self = self, self.recoveryMode == .iceRestarting, self.recoveringCall === call else { return }
            Logger.log.i(message: "[CALL-RECOVERY] ICE restart answer applied")
            self.cancelIceRestartTimeout()

            // An SDP response only proves signaling succeeded. Discard the
            // old transport's RTP baseline so recovery requires packet growth
            // sampled after the answer is applied.
            self.lastInboundRtpPacketCount = nil
            self.lastInboundRtpProgressAt = nil
            self.recoveryMode = .verifyingMedia
            self.mediaVerificationStartedAt = Date()
            call.setCallReportMediaVerificationSamplingEnabled(true)
            self.startMediaVerificationTimeout(for: call)
        }
    }

    func iceRestartRequestDidFail(for call: Call, error: Error) {
        executeOnQueue { [weak self] in
            guard let self = self, self.recoveryMode == .iceRestarting, self.recoveringCall === call else { return }
            Logger.log.e(message: "[CALL-RECOVERY] ICE restart failed: \(error.localizedDescription)")
            self.beginReattach()
        }
    }

    func callStateDidChange(_ call: Call) {
        executeOnQueue { [weak self] in
            guard let self = self else { return }

            switch call.callState {
            case .ACTIVE:
                if self.recoveryMode == .reattaching {
                    Logger.log.i(message: "[CALL-RECOVERY] Reattached call is active")
                    self.finishRecovery()
                } else if self.recoveryMode != .idle, self.recoveringCall !== call {
                    // A replacement call, or the recovering one is gone: the
                    // recovery has nothing left to do. `recoveringCall` is
                    // weak, so a replaced call reads as nil here.
                    Logger.log.i(message: "[CALL-RECOVERY] A different call is active; ending the recovery")
                    self.finishRecovery()
                }
                self.monitoredActiveCall = call
                self.startSignalingHealthChecks()
                self.resetInboundRtpTracking()
            case .HELD:
                if self.monitoredActiveCall === call {
                    self.resetInboundRtpTracking()
                }
            case .DONE, .DROPPED:
                self.cancelPeerDisconnectedRecovery()
                call.setCallReportMediaVerificationSamplingEnabled(false)
                if self.monitoredActiveCall === call {
                    self.stopSignalingHealthChecks()
                    self.monitoredActiveCall = nil
                }
                if self.recoveringCall === call {
                    self.finishRecovery()
                }
            default:
                break
            }
        }
    }

    private func requestRecovery(for call: Call, trigger: String) {
        cancelPeerDisconnectedRecovery()
        guard call.callState == .ACTIVE else {
            Logger.log.i(message: "[CALL-RECOVERY] Ignoring \(trigger); call is \(call.callState.value)")
            return
        }
        guard recoveryMode == .idle || recoveryMode == .watchingMedia else {
            Logger.log.i(message: "[CALL-RECOVERY] Ignoring \(trigger); recovery is already in progress")
            return
        }
        if recoveryMode == .watchingMedia {
            // The peer did not survive the path change on its own.
            Logger.log.w(message: "[CALL-RECOVERY] \(trigger) while watching media after a path change")
            cancelMediaWatchTimeout()
            recoveryMode = .idle
        }

        recoveringCall = call
        shouldEvaluateRelayFallback = true
        if !isSignalingAvailable() {
            Logger.log.w(message: "[CALL-RECOVERY] \(trigger) with signaling unavailable; starting reconnect/reattach")
            beginReattach()
        } else if hasRecentSignalingActivity() {
            Logger.log.w(message: "[CALL-RECOVERY] \(trigger) with signaling available; starting ICE restart")
            startIceRestartRecovery(for: call)
        } else {
            Logger.log.w(message: "[CALL-RECOVERY] \(trigger) with stale signaling; probing before ICE restart")
            startSignalingProbe(for: call, purpose: .mediaRecovery)
        }
    }

    private func hasRecentSignalingActivity() -> Bool {
        let now = Date()
        return now.timeIntervalSince(lastInboundSignalingActivity) <= recentInboundActivityThreshold
            && now.timeIntervalSince(lastConfirmedOutboundActivity) <= confirmedOutboundActivityThreshold
    }

    private func startIceRestartRecovery(for call: Call) {
        recoveryMode = .iceRestarting
        iceRestartStartedAt = Date()
        startIceRestartTimeout(for: call)
        startIceRestart(call)
    }

    private func startSignalingProbe(for call: Call, purpose: SignalingProbePurpose) {
        guard let probeId = sendSignalingProbe() else {
            Logger.log.e(message: "[CALL-RECOVERY] Unable to send signaling health probe")
            beginReattach()
            return
        }

        cancelSignalingProbeTimeout()
        recoveryMode = .probing
        pendingSignalingProbeId = probeId
        pendingSignalingProbePurpose = purpose
        let workItem = DispatchWorkItem { [weak self, weak call] in
            guard let self = self,
                  let call = call,
                  self.recoveryMode == .probing,
                  self.recoveringCall === call else { return }
            Logger.log.e(message: "[CALL-RECOVERY] Signaling health probe timed out after \(self.signalingProbeTimeout)s")
            self.beginReattach()
        }
        signalingProbeTimeoutWorkItem = workItem
        queue.asyncAfter(deadline: .now() + signalingProbeTimeout, execute: workItem)
    }

    private func startSignalingHealthChecks() {
        guard signalingHealthCheckTimer == nil else { return }
        let timer = DispatchSource.makeTimerSource(queue: queue)
        timer.schedule(deadline: .now() + signalingHealthCheckInterval, repeating: signalingHealthCheckInterval)
        timer.setEventHandler { [weak self] in
            self?.checkSignalingHealth()
        }
        signalingHealthCheckTimer = timer
        timer.resume()
    }

    private func stopSignalingHealthChecks() {
        signalingHealthCheckTimer?.setEventHandler {}
        signalingHealthCheckTimer?.cancel()
        signalingHealthCheckTimer = nil
    }

    private func startPeerDisconnectedRecoveryDelay(for call: Call) {
        guard recoveryMode == .idle else { return }
        cancelPeerDisconnectedRecovery()
        let workItem = DispatchWorkItem { [weak self, weak call] in
            guard let self = self,
                  let call = call,
                  self.recoveryMode == .idle,
                  call.callState == .ACTIVE else { return }
            Logger.log.w(message: "[CALL-RECOVERY] Peer remained disconnected for \(self.peerDisconnectedRecoveryDelay)s; starting recovery")
            self.requestRecovery(for: call, trigger: "peer_connection_disconnected")
        }
        peerDisconnectedRecoveryWorkItem = workItem
        queue.asyncAfter(deadline: .now() + peerDisconnectedRecoveryDelay, execute: workItem)
    }

    private func cancelPeerDisconnectedRecovery() {
        peerDisconnectedRecoveryWorkItem?.cancel()
        peerDisconnectedRecoveryWorkItem = nil
    }

    private func resetInboundRtpTracking() {
        lastInboundRtpPacketCount = nil
        lastInboundRtpProgressAt = Date()
        iceRestartStartedAt = nil
        mediaVerificationStartedAt = nil
    }

    /// Receives the inbound packet counter from the call-report collector's
    /// existing WebRTC sample. This intentionally does not request a second
    /// full stats report solely for media health.
    func inboundRtpSampleReceived(_ packetsReceived: Int, for call: Call) {
        executeOnQueue { [weak self] in
            self?.recordInboundRtpPackets(packetsReceived, for: call)
        }
    }

    private func recordInboundRtpPackets(_ packetsReceived: Int, for call: Call) {
        guard (monitoredActiveCall === call && call.callState == .ACTIVE) || recoveringCall === call else { return }

        let now = Date()
        let previousPacketCount = lastInboundRtpPacketCount
        lastInboundRtpPacketCount = packetsReceived

        guard let previousPacketCount = previousPacketCount else {
            lastInboundRtpProgressAt = now
            return
        }

        guard packetsReceived > previousPacketCount else { return }

        lastInboundRtpProgressAt = now
        switch recoveryMode {
        case .verifyingMedia:
            if let verificationStartedAt = mediaVerificationStartedAt, now >= verificationStartedAt {
                Logger.log.i(message: "[CALL-RECOVERY] Inbound RTP resumed after ICE restart")
                finishRecovery(recoveredInPlace: call)
            }
        case .watchingMedia:
            let elapsed = pathChangedAt.map { now.timeIntervalSince($0) } ?? 0
            Logger.log.i(message: "[CALL-RECOVERY] Inbound RTP flowing \(String(format: "%.1f", elapsed))s after the path change; nothing was replaced")
            finishRecovery()
        case .awaitingSignaling:
            // Media healed before signaling is back. Remembered, not acted
            // on: the socket still has to be replaced, and
            // `signalingDidReconnect` ends the recovery then.
            if mediaHealedAt == nil {
                mediaHealedAt = now
                Logger.log.i(message: "[CALL-RECOVERY] Inbound RTP flowing on the new path before signaling is back")
            }
        case .iceRestarting:
            // The restart offer is in flight but the kept peer found the
            // new path by itself. The answer, when it lands, is applied
            // anyway; the call does not have to wait for it.
            if let startedAt = iceRestartStartedAt, now >= startedAt {
                Logger.log.i(message: "[CALL-RECOVERY] Inbound RTP resumed while the ICE restart was in flight")
                finishRecovery(recoveredInPlace: call)
            }
        default:
            break
        }
    }

    private func checkSignalingHealth() {
        guard recoveryMode == .idle,
              let call = monitoredActiveCall,
              call.callState == .ACTIVE else { return }

        guard isSignalingAvailable() else {
            Logger.log.w(message: "[CALL-RECOVERY] Signaling socket is unavailable during an active call")
            recoveringCall = call
            shouldEvaluateRelayFallback = false
            beginReattach()
            return
        }

        let now = Date()
        let inboundIsStale = now.timeIntervalSince(lastInboundSignalingActivity) >= staleInboundActivityThreshold
        let outboundIsStale = now.timeIntervalSince(lastConfirmedOutboundActivity) >= confirmedOutboundActivityThreshold
        guard inboundIsStale || outboundIsStale else { return }

        Logger.log.w(message: "[CALL-RECOVERY] Signaling activity is stale during an active call; probing")
        recoveringCall = call
        shouldEvaluateRelayFallback = false
        startSignalingProbe(for: call, purpose: .healthCheck)
    }

    private func startIceRestartTimeout(for call: Call) {
        cancelIceRestartTimeout()
        let workItem = DispatchWorkItem { [weak self, weak call] in
            guard let self = self,
                  let call = call,
                  self.recoveryMode == .iceRestarting,
                  self.recoveringCall === call else { return }
            Logger.log.e(message: "[CALL-RECOVERY] ICE restart timed out after \(self.iceRestartTimeout)s")
            self.beginReattach()
        }
        iceRestartTimeoutWorkItem = workItem
        queue.asyncAfter(deadline: .now() + iceRestartTimeout, execute: workItem)
    }

    private func startMediaVerificationTimeout(for call: Call) {
        cancelMediaVerificationTimeout()
        let workItem = DispatchWorkItem { [weak self, weak call] in
            guard let self = self,
                  let call = call,
                  self.recoveryMode == .verifyingMedia,
                  self.recoveringCall === call else { return }
            Logger.log.e(message: "[CALL-RECOVERY] ICE restart did not restore inbound RTP after \(self.postIceRestartMediaTimeout)s; reattaching")
            self.beginReattach()
        }
        mediaVerificationTimeoutWorkItem = workItem
        queue.asyncAfter(deadline: .now() + postIceRestartMediaTimeout, execute: workItem)
    }

    private func startPathRecoveryTimeout(for call: Call) {
        cancelPathRecoveryTimeout()
        let workItem = DispatchWorkItem { [weak self, weak call] in
            guard let self = self, self.recoveryMode == .awaitingSignaling else { return }
            guard let call = call, self.recoveringCall === call else {
                // The call this was about is gone; nothing to reattach.
                self.finishRecovery()
                return
            }
            Logger.log.e(message: "[CALL-RECOVERY] Signaling did not return within \(self.pathRecoveryTimeout)s of the path change; reattaching")
            self.beginReattach()
        }
        pathRecoveryTimeoutWorkItem = workItem
        queue.asyncAfter(deadline: .now() + pathRecoveryTimeout, execute: workItem)
    }

    private func cancelPathRecoveryTimeout() {
        pathRecoveryTimeoutWorkItem?.cancel()
        pathRecoveryTimeoutWorkItem = nil
    }

    private func startMediaWatchTimeout(for call: Call) {
        cancelMediaWatchTimeout()
        let workItem = DispatchWorkItem { [weak self, weak call] in
            guard let self = self, self.recoveryMode == .watchingMedia else { return }
            guard let call = call, self.recoveringCall === call else {
                self.finishRecovery()
                return
            }
            Logger.log.w(message: "[CALL-RECOVERY] No inbound RTP \(self.mediaWatchTimeout)s after the path change; restarting ICE")
            self.cancelMediaWatchTimeout()
            self.shouldEvaluateRelayFallback = true
            self.startIceRestartRecovery(for: call)
        }
        mediaWatchTimeoutWorkItem = workItem
        queue.asyncAfter(deadline: .now() + mediaWatchTimeout, execute: workItem)
    }

    private func cancelMediaWatchTimeout() {
        mediaWatchTimeoutWorkItem?.cancel()
        mediaWatchTimeoutWorkItem = nil
    }

    private func beginReattach() {
        recoveringCall?.setCallReportMediaVerificationSamplingEnabled(false)
        cancelRecoveryTimeouts()
        recoveryMode = .reattaching
        guard shouldEvaluateRelayFallback, let call = recoveringCall else {
            requestReattach(false)
            return
        }

        shouldEvaluateRelayFallback = false
        shouldForceRelayForRecovery(call) { [weak self] shouldForceRelay in
            self?.executeOnQueue { [weak self] in
                guard let self = self, self.recoveryMode == .reattaching else { return }
                if shouldForceRelay {
                    Logger.log.w(message: "[CALL-RECOVERY] Failed VPN direct path; forcing relay for replacement call")
                }
                self.requestReattach(shouldForceRelay)
            }
        }
    }

    /// Ends the recovery. `recoveredInPlace` names a call whose own peer
    /// came through, so the client puts it back to ACTIVE itself; a
    /// reattached call reports ACTIVE on its own.
    private func finishRecovery(recoveredInPlace call: Call? = nil) {
        recoveringCall?.setCallReportMediaVerificationSamplingEnabled(false)
        cancelRecoveryTimeouts()
        recoveringCall = nil
        shouldEvaluateRelayFallback = false
        recoveryMode = .idle
        iceRestartStartedAt = nil
        mediaVerificationStartedAt = nil
        pathChangedAt = nil
        mediaHealedAt = nil
        if let call = call {
            didRecoverInPlace(call)
        }
    }

    private func cancelRecoveryTimeouts() {
        cancelIceRestartTimeout()
        cancelSignalingProbeTimeout()
        cancelMediaVerificationTimeout()
        cancelPathRecoveryTimeout()
        cancelMediaWatchTimeout()
    }

    private func cancelIceRestartTimeout() {
        iceRestartTimeoutWorkItem?.cancel()
        iceRestartTimeoutWorkItem = nil
    }

    private func cancelSignalingProbeTimeout() {
        signalingProbeTimeoutWorkItem?.cancel()
        signalingProbeTimeoutWorkItem = nil
        pendingSignalingProbeId = nil
        pendingSignalingProbePurpose = nil
    }

    private func cancelMediaVerificationTimeout() {
        mediaVerificationTimeoutWorkItem?.cancel()
        mediaVerificationTimeoutWorkItem = nil
    }

    private func executeOnQueue(_ block: @escaping () -> Void) {
        queue.async(execute: block)
    }
}
