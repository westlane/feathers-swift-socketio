//
//  SocketProvider.swift
//  FeathersSwiftSocketIO
//
//  Created by Brendan Conron on 5/16/17.
//  Copyright © 2017 FeathersJS. All rights reserved.
//

import SocketIO
import Foundation
import Feathers
import ReactiveSwift

public final class SocketProvider: Provider {
    
    public let baseURL: URL
    
    public var supportsRealtimeEvents: Bool {
        return true
    }
    
    /// SocketIO client.
    private let client: SocketIOClient
    
    /// Socket manager - keep strong reference to prevent deallocation during async ops
    private let manager: SocketManager
    
    /// Socket timeout for `connect` and all emits.
    private let timeout: Double
    
    /// Request serialization to prevent duplicate ack IDs
    /// The socket.io library's ack ID generator is not thread-safe for concurrent requests
    private let requestSemaphore = DispatchSemaphore(value: 1) // Only 1 request at a time
    
    /// Socket provider initializer.
    ///
    /// - Parameters:
    ///   - baseURL: Socket url.
    ///   - configuration: Socket configuration object. See `SocketIO` for more details
    /// on the possible options.
    ///   - timeout: Socket timeout.
    public init(manager: SocketManager, timeout: Double = 5) {
        self.baseURL = manager.socketURL
        self.timeout = timeout
        self.manager = manager
        client = manager.defaultSocket
    }
    
    public func setup(app: Feathers) {
        // Attempt to authenticate using a previously stored token once the client connects.
        // This is now safe thanks to request serialization preventing duplicate ack IDs.
        // If manual authentication clears the token, this won't run.
        client.once("connect") { [weak app = app, weak self] data, ack in
            guard let vSelf = self else { return }
            guard let vApp = app else { return }
            guard let accessToken = vApp.authenticationStorage.accessToken else { return }
            vSelf.emit(to: "authenticate", with: [
                "strategy": vApp.authenticationConfiguration.jwtStrategy,
                "accessToken": accessToken
                ])
                .on(failed: { _ in
                    vApp.authenticationStorage.accessToken = accessToken
                }, value: { value in
                    if case let .object(object) = value.data,
                        let json = object as? [String: Any],
                        let accessToken = json["accessToken"] as? String {
                        vApp.authenticationStorage.accessToken = accessToken
                    }
                })
                .start()
        }
        
        client.connect(timeoutAfter: timeout) {
            print("feathers socket failed to connect")
        }
    }
    
    public func request(endpoint: Endpoint) -> SignalProducer<Response, AnyFeathersError> {
        // FeathersJS Socket.IO format varies by method (second argument is the service path string):
        // find: emit('find', servicePath, params)
        // create: emit('create', servicePath, data, params)
        // patch: emit('patch', servicePath, id, data, params)
        let method = endpoint.method.socketRequestPath
        let serviceName = endpoint.path
        let socketParams = endpoint.method.socketData  // Array of parameters
        
        return SignalProducer { observer, lifetime in
            // Serialize socket requests to ensure reliable operation
            // The socket.io library expects sequential request handling for proper ack management
            self.requestSemaphore.wait()
            
            // Strongly capture client and manager to prevent deallocation during async operations
            let client = self.client
            let manager = self.manager
            
            // Ensure we always release the semaphore
            let releaseOnce: () -> Void = {
                self.requestSemaphore.signal()
            }
            var hasReleased = false
            let releaseSemaphore = {
                if !hasReleased {
                    hasReleased = true
                    releaseOnce()
                }
            }
            
            // Check if socket is actually connected
            guard client.status == .connected else {
                releaseSemaphore()
                observer.send(error: AnyFeathersError(FeathersNetworkError.unknown))
                return
            }
            
            // Note: Authentication is handled at the socket level via the auto-auth on connect.
            // No need to pass auth tokens with each request in Feathers v4 style.
            
            // Emit with correct number of parameters based on socketParams count
            let ackCallback: OnAckCallback
            switch socketParams.count {
            case 1:
                ackCallback = client.emitWithAck(method, serviceName, socketParams[0] ?? [:])
            case 2:
                ackCallback = client.emitWithAck(method, serviceName, socketParams[0] ?? [:], socketParams[1] ?? [:])
            case 3:
                ackCallback = client.emitWithAck(method, serviceName, socketParams[0] ?? [:], socketParams[1] ?? [:], socketParams[2] ?? [:])
            default:
                releaseSemaphore()
                observer.send(error: AnyFeathersError(FeathersNetworkError.unknown))
                return
            }

            
            ackCallback.timingOut(after: 10) { [manager, client] response in
                // Release semaphore when response arrives (or times out)
                releaseSemaphore()
                
                // Capture manager and client strongly to keep SocketAckManager alive
                _ = manager
                _ = client
                if response.isEmpty {
                    observer.send(error: AnyFeathersError(FeathersNetworkError.unknown))
                } else if let errorData = response.first as? [String: Any],
                          let errorCode = errorData["code"] as? Int {
                    let detail = FeathersHTTPErrorDetail.make(statusCode: errorCode, json: errorData)
                    observer.send(error: AnyFeathersError(FeathersNetworkError.underlying(detail)))
                } else {
                    // Success response - FeathersJS returns [null, data] format
                    // The actual data is in response[1], response[0] is null for success
                    if response.count > 1 {
                        let responseData = response[1]
                        // Handle different response data types safely
                        if let dictData = responseData as? [String: Any] {
                            // Check if this is a paginated response (has total, limit, skip, data)
                            if let pagination = self.parsePagination(data: dictData),
                               let dataArray = dictData["data"] as? [Any] {
                                let jsonResponse = Response(pagination: pagination, data: .list(dataArray))
                                observer.send(value: jsonResponse)
                                observer.sendCompleted()
                            } else {
                                // Not paginated, return as object
                                let jsonResponse = Response(pagination: nil, data: .object(dictData))
                                observer.send(value: jsonResponse)
                                observer.sendCompleted()
                            }
                        } else if let arrayData = responseData as? [[String: Any]] {
                            let jsonResponse = Response(pagination: nil, data: .list(arrayData))
                            observer.send(value: jsonResponse)
                            observer.sendCompleted()
                        } else {
                            // Fallback for other types
                            let jsonResponse = Response(pagination: nil, data: .object(["result": responseData]))
                            observer.send(value: jsonResponse)
                            observer.sendCompleted()
                        }
                    } else if let responseData = response.first {
                        // Handle single response data
                        if let dictData = responseData as? [String: Any] {
                            // Check if this is a paginated response (has total, limit, skip, data)
                            if let pagination = self.parsePagination(data: dictData), 
                               let dataArray = dictData["data"] as? [Any] {
                                let jsonResponse = Response(pagination: pagination, data: .list(dataArray))
                                observer.send(value: jsonResponse)
                                observer.sendCompleted()
                            } else {
                                // Not paginated, return as object
                                let jsonResponse = Response(pagination: nil, data: .object(dictData))
                                observer.send(value: jsonResponse)
                                observer.sendCompleted()
                            }
                        } else if let arrayData = responseData as? [[String: Any]] {
                            let jsonResponse = Response(pagination: nil, data: .list(arrayData))
                            observer.send(value: jsonResponse)
                            observer.sendCompleted()
                        } else {
                            // Fallback for other types
                            let jsonResponse = Response(pagination: nil, data: .object(["result": responseData]))
                            observer.send(value: jsonResponse)
                            observer.sendCompleted()
                        }
                    } else {
                        observer.send(error: AnyFeathersError(FeathersNetworkError.unknown))
                    }
                }
            }
        }
    }
    
    public func authenticate(_ path: String, credentials: [String : Any]) -> SignalProducer<Response, AnyFeathersError> {
        // Feathers v5: Use service-based authentication instead of deprecated "authenticate" event
        // Modern approach: Call the authentication service using standard service methods:
        //   client.service("authentication").request(.create(data: credentials))
        //
        // Keeping this method for backward compatibility but it should not be used
        return emit(to: "authenticate", with: credentials)
    }
    
    public func logout(path: String) -> SignalProducer<Response, AnyFeathersError> {
        // Feathers v5: Logout should also use service-based approach
        // Modern: client.service("authentication").request(.remove(id: nil))
        // But keeping legacy event for backward compatibility
        return emit(to: "logout", with: [])
    }
    
    /// Emit data to a given path.
    ///
    /// - Parameters:
    ///   - path: Path to emit on.
    ///   - data: Data to emit.
    ///   - completion: Completion callback.
    private func emit(to path: String, with data: SocketData) -> SignalProducer<Response, AnyFeathersError> {
        return SignalProducer { observer, disposable in
            // Strongly capture manager and client to prevent deallocation during async operations
            let manager = self.manager
            let client = self.client
            
            if client.status == .connecting {
                client.once("connect") { _,_  in
                    client.emitWithAck(path, data).timingOut(after: self.timeout) { [manager, client] data in
                        // Keep manager and client alive throughout callback lifecycle
                        _ = manager
                        _ = client
                        let result = self.handleResponseData(data: data)
                        if let error = result.error {
                            observer.send(error: error)
                        } else if let response = result.value {
                            observer.send(value: response)
                            observer.sendCompleted()  // Complete the signal
                        } else {
                            observer.send(error: AnyFeathersError(FeathersNetworkError.unknown))
                        }
                    }
                }
            } else {
                client.emitWithAck(path, data).timingOut(after: self.timeout) { [manager, client] data in
                    // Keep manager and client alive throughout callback lifecycle
                    _ = manager
                    _ = client
                    let result = self.handleResponseData(data: data)
                    if let error = result.error {
                        observer.send(error: error)
                    } else if let response = result.value {
                        observer.send(value: response)
                        observer.sendCompleted()  // Complete the signal
                    } else {
                        observer.send(error: AnyFeathersError(FeathersNetworkError.unknown))
                    }
                }
            }
            
        }
    }
    
    /// Parse and handle socket response data.
    ///
    /// - Parameter data: Socket response data.
    /// - Returns: Result object with error or response.
    private func handleResponseData(data: [Any]) -> Result<Response, AnyFeathersError> {
        if let noAck = data.first as? String, noAck == "NO ACK" {
            return .failure(AnyFeathersError(FeathersNetworkError.notFound))
        } else if let errorData = data.first as? [String: Any], let code = errorData["code"] as? Int {
            let detail = FeathersHTTPErrorDetail.make(statusCode: code, json: errorData)
            return .failure(AnyFeathersError(FeathersNetworkError.underlying(detail)))
        } else if let jsonObject = data.last as? [String: Any] {
            if let pagination = parsePagination(data: jsonObject), let data = jsonObject["data"] as? [Any] {
                return .success(Response(pagination: pagination, data: .list(data)))
            }
            return .success(Response(pagination: nil, data: .object(jsonObject)))
        } else if let jsonArray = data.last as? [Any] {
            return .success(Response(pagination: nil, data: .list(jsonArray)))
        }
        return .failure(AnyFeathersError(FeathersNetworkError.unknown))
    }
    
    /// Parse pagination data if any.
    ///
    /// - Parameter data: Data to parse from.
    /// - Returns: Paginiation object or nil.
    private func parsePagination(data: [String: Any]) -> Pagination? {
        guard
            let limit = data["limit"] as? Int,
            let skip = data["skip"] as? Int,
            let total = data["total"] as? Int else {
                return nil
        }
        return Pagination(total: total, limit: limit, skip: skip)
    }
    
    // MARK: - RealTimeProvider
    
    public func on(event: String) -> Signal<[String: Any], Never> {
        return Signal { [weak client = client] observer, lifetime in
            guard let vClient = client else {
                observer.sendInterrupted()
                return
            }
            vClient.on(event, callback: { data, _ in
                guard let object = data.first as? [String: Any] else { 
                    return 
                }
                observer.send(value: object)
            })
            let disposable = AnyDisposable {
                vClient.off(event)
            }
            lifetime += disposable
        }
    }
    
    public func once(event: String) -> Signal<[String: Any], Never> {
        return Signal { [weak client = client] observer, lifetime in
            guard let vClient = client else {
                observer.sendInterrupted()
                return
            }
            vClient.once(event, callback: { data, _ in
                guard let object = data.first as? [String: Any] else { return }
                observer.send(value: object)
                observer.sendCompleted()
            })
            let disposable = AnyDisposable {
                vClient.off(event)
            }
            lifetime += disposable
        }
    }
    
    public func off(event: String) {
        client.off(event)
    }
    
    // MARK: - Deinit
    
    deinit {
        client.disconnect()
    }
    
}

fileprivate extension Service.Method {
    
    var socketRequestPath: String {
        switch self {
        case .find: return "find"
        case .get: return "get"
        case .create: return "create"
        case .update: return "update"
        case .patch: return "patch"
        case .remove: return "remove"  // Method is 'remove', event is 'removed'
        }
    }
    
    var socketData: [SocketData?] {
        switch self {
        case .find(let query):
            return [query?.serialize() ?? [:]]
        case .get(let id, let query):
            return [id, query?.serialize() ?? [:]]
        case .create(let data, let query):
            return [data, query?.serialize() ?? [:]]
        case .update(let id, let data, let query),
             .patch(let id, let data, let query):
            return [id ?? nil, data, query?.serialize() ?? [:]]
        case .remove(let id, let query):
            return [id ?? nil, query?.serialize()]
        }
    }
    
}
