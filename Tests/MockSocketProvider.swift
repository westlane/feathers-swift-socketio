//
//  MockSocketProvider.swift
//  FeathersSwiftSocketIOTests
//
//  Created by Tests on 2025.
//  Copyright © 2025 FeathersJS. All rights reserved.
//

import Foundation
import Dispatch
import ReactiveSwift
import SocketIO
import Feathers
@testable import FeathersSwiftSocketIO

/// Mock socket provider for testing purposes
class MockSocketProvider: Provider {
    
    var baseURL: URL {
        return URL(string: "http://localhost:3030")!
    }
    
    var supportsRealtimeEvents: Bool {
        return true
    }
    
    private let stubbedData: [String: Any]
    private var eventHandlers: [String: ([String: Any]) -> Void] = [:]
    
    init(data: [String: Any] = [:]) {
        self.stubbedData = data
    }
    
    func setup(app: Feathers) {
        // Mock setup - no-op
    }
    
    func request(endpoint: Endpoint) -> SignalProducer<Response, AnyFeathersError> {
        let data = self.stubbedData
        return SignalProducer { observer, disposable in
            let response = Response(pagination: nil, data: .object(data))
            observer.send(value: response)
            observer.sendCompleted()
        }
    }
    
    func authenticate(_ path: String, credentials: [String : Any]) -> SignalProducer<Response, AnyFeathersError> {
        return SignalProducer { observer, disposable in
            let response = Response(pagination: nil, data: .object(["accessToken": "mock_token"]))
            observer.send(value: response)
            observer.sendCompleted()
        }
    }
    
    func logout(path: String) -> SignalProducer<Response, AnyFeathersError> {
        return SignalProducer { observer, disposable in
            let response = Response(pagination: nil, data: .object([:]))
            observer.send(value: response)
            observer.sendCompleted()
        }
    }
    
    func on(event: String) -> Signal<[String: Any], Never> {
        return Signal { observer, lifetime in
            self.eventHandlers[event] = { data in
                observer.send(value: data)
            }
            
            let disposable = AnyDisposable {
                self.eventHandlers.removeValue(forKey: event)
            }
            lifetime += disposable
        }
    }
    
    func once(event: String) -> Signal<[String: Any], Never> {
        return Signal { observer, lifetime in
            self.eventHandlers[event] = { data in
                observer.send(value: data)
                observer.sendCompleted()
                self.eventHandlers.removeValue(forKey: event)
            }
            
            let disposable = AnyDisposable {
                self.eventHandlers.removeValue(forKey: event)
            }
            lifetime += disposable
        }
    }
    
    func off(event: String) {
        eventHandlers.removeValue(forKey: event)
    }
    
    // MARK: - Test Helpers
    
    func simulateEvent(_ event: String, data: [String: Any]) {
        eventHandlers[event]?(data)
    }
}
