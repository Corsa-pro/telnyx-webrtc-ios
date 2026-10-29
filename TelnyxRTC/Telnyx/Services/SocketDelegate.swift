//
//  SocketDelegate.swift
//  TelnyxRTC
//
//  Created by Guillermo Battistel on 02/03/2021.
//  Copyright © 2021 Telnyx LLC. All rights reserved.
//

import Foundation

protocol SocketDelegate: AnyObject {
    func onSocketConnected()
    func onSocketDisconnected(reconnect:Bool,region:Region?)
    func onSocketError(error: Error)
    func onMessageReceived(message: String)
    /// The transport's own view of the path: false means the connection
    /// cannot carry data right now, before any timeout has noticed.
    func onSocketViabilityChanged(viable: Bool)
    /// The transport has seen a better path and advises reconnecting on it.
    func onSocketReconnectSuggested()
}

extension SocketDelegate {
    func onSocketViabilityChanged(viable: Bool) {}
    func onSocketReconnectSuggested() {}
}

/// Takes the events of a signaling socket that has been replaced. Its
/// messages still reach the client — the server may answer on it until the
/// new socket is registered — but its closing is nobody's business: a
/// retired socket's close must not reset the session or start a reconnect,
/// which is how two sockets came to log in with one session.
final class RetiredSocketSink: SocketDelegate {
    private weak var client: TxClient?

    init(client: TxClient) {
        self.client = client
    }

    func onSocketConnected() {}

    func onSocketDisconnected(reconnect: Bool, region: Region?) {
        Logger.log.i(message: "Socket:: retired socket closed")
    }

    func onSocketError(error: Error) {
        Logger.log.i(message: "Socket:: retired socket error: \(error.localizedDescription)")
    }

    func onMessageReceived(message: String) {
        client?.onMessageReceived(message: message)
    }
}
