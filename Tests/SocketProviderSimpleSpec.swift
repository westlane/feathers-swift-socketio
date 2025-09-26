//
//  SocketProviderSimpleTests.swift
//  FeathersSwiftSocketIOTests
//
//  Created by Tests on 2025.
//  Copyright © 2025 FeathersJS. All rights reserved.
//

import Foundation
import XCTest
import ReactiveSwift
import SocketIO
import Feathers
@testable import FeathersSwiftSocketIO

class SocketProviderSimpleTests: XCTestCase {
    
    func testInitializeWithCorrectProperties() {
        let url = URL(string: "http://localhost:3030")!
        let manager = SocketManager(socketURL: url, config: [.log(false), .compress])
        let provider = SocketProvider(manager: manager, timeout: 5.0)
        
        XCTAssertEqual(provider.baseURL.absoluteString, "http://localhost:3030")
        XCTAssertTrue(provider.supportsRealtimeEvents)
    }
    
    func testHandleDifferentTimeoutValues() {
        let url = URL(string: "http://localhost:3030")!
        let manager = SocketManager(socketURL: url, config: [.log(false)])
        
        let provider1 = SocketProvider(manager: manager, timeout: 1.0)
        let provider2 = SocketProvider(manager: manager, timeout: 10.0)
        
        XCTAssertEqual(provider1.baseURL, provider2.baseURL)
        XCTAssertEqual(provider1.supportsRealtimeEvents, provider2.supportsRealtimeEvents)
    }
    
    func testHandleSocketManagerConfiguration() {
        let url = URL(string: "ws://example.com:8080")!
        let config: SocketIOClientConfiguration = [
            .log(false),
            .compress,
            .connectParams(["token": "test-token"])
        ]
        let manager = SocketManager(socketURL: url, config: config)
        let provider = SocketProvider(manager: manager)
        
        XCTAssertEqual(provider.baseURL.absoluteString, "ws://example.com:8080")
        XCTAssertTrue(provider.supportsRealtimeEvents)
    }
    
    func testCreateProviderWithDefaultTimeout() {
        let url = URL(string: "http://localhost:3030")!
        let manager = SocketManager(socketURL: url, config: [.log(false)])
        let provider = SocketProvider(manager: manager) // Using default timeout
        
        XCTAssertNotNil(provider.baseURL)
        XCTAssertTrue(provider.supportsRealtimeEvents)
    }
}
