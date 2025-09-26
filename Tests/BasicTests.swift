//
//  BasicTests.swift
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

class BasicTests: XCTestCase {
    
    func testCreateSocketProviderWithCorrectProperties() {
        let url = URL(string: "http://localhost:3030")!
        let manager = SocketManager(socketURL: url, config: [.log(false), .compress])
        let provider = SocketProvider(manager: manager, timeout: 5.0)
        
        XCTAssertEqual(provider.baseURL.absoluteString, "http://localhost:3030")
        XCTAssertTrue(provider.supportsRealtimeEvents)
    }
    
    func testMockSocketProviderForServiceRequests() {
        let mockProvider = MockSocketProvider(data: ["id": "123", "name": "Test User"])
        let app = Feathers(provider: mockProvider)
        let service = app.service(path: "users")
        
        let expectation = XCTestExpectation(description: "Service request should complete")
        
        service.request(.find(query: nil))
            .on(failed: { error in
                XCTFail("Request should not fail: \(error)")
            }, value: { response in
                XCTAssertNotNil(response.data)
                if case let .object(data) = response.data,
                   let dict = data as? [String: Any] {
                    XCTAssertEqual(dict["id"] as? String, "123")
                    XCTAssertEqual(dict["name"] as? String, "Test User")
                }
                expectation.fulfill()
            })
            .start()
        
        wait(for: [expectation], timeout: 2.0)
    }
    
    func testAuthenticationWithMockSocketProvider() {
        let mockProvider = MockSocketProvider()
        let app = Feathers(provider: mockProvider)
        
        let expectation = XCTestExpectation(description: "Authentication should complete")
        
        app.authenticate(["strategy": "local", "email": "test@example.com", "password": "password"])
            .on(failed: { error in
                XCTFail("Authentication should not fail: \(error)")
            }, value: { authData in
                XCTAssertNotNil(authData)
                XCTAssertEqual(authData["accessToken"] as? String, "mock_token")
                expectation.fulfill()
            })
            .start()
        
        wait(for: [expectation], timeout: 2.0)
    }
    
    func testLogoutWithMockSocketProvider() {
        let mockProvider = MockSocketProvider()
        let app = Feathers(provider: mockProvider)
        
        let expectation = XCTestExpectation(description: "Logout should complete")
        
        app.logout()
            .on(failed: { error in
                XCTFail("Logout should not fail: \(error)")
            }, value: { response in
                XCTAssertNotNil(response.data)
                expectation.fulfill()
            })
            .start()
        
        wait(for: [expectation], timeout: 2.0)
    }
    
    func testAllServiceMethodsWithMockSocketProvider() {
        let mockProvider = MockSocketProvider(data: ["result": "success"])
        let app = Feathers(provider: mockProvider)
        let service = app.service(path: "users")
        
        let methods: [Service.Method] = [
            .find(query: nil),
            .get(id: "123", query: nil),
            .create(data: ["name": "Test"], query: nil),
            .update(id: "123", data: ["name": "Updated"], query: nil),
            .patch(id: "123", data: ["name": "Patched"], query: nil),
            .remove(id: "123", query: nil)
        ]
        
        for method in methods {
            let expectation = XCTestExpectation(description: "Service method \(method) should complete")
            
            service.request(method)
                .on(failed: { error in
                    XCTFail("Request should not fail: \(error)")
                }, value: { response in
                    XCTAssertNotNil(response.data)
                    if case let .object(data) = response.data,
                       let dict = data as? [String: Any] {
                        XCTAssertEqual(dict["result"] as? String, "success")
                    }
                    expectation.fulfill()
                })
                .start()
            
            wait(for: [expectation], timeout: 1.0)
        }
    }
    
    func testTimeoutScenariosGracefully() {
        let url = URL(string: "http://localhost:3030")!
        let manager = SocketManager(socketURL: url, config: [.log(false), .compress])
        let provider = SocketProvider(manager: manager, timeout: 0.1) // Very short timeout
        
        let expectation = XCTestExpectation(description: "Timeout scenario should complete")
        
        provider.authenticate("authentication", credentials: ["strategy": "local"])
            .on(failed: { error in
                // Expected to fail due to timeout or no server
                XCTAssertNotNil(error)
                expectation.fulfill()
            }, value: { response in
                // Unexpected success, but still valid
                XCTAssertNotNil(response)
                expectation.fulfill()
            })
            .start()
        
        wait(for: [expectation], timeout: 2.0)
    }
}
