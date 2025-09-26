//
//  MockProviderTests.swift
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

class MockProviderTests: XCTestCase {
    
    var mockProvider: MockSocketProvider!
    var app: Feathers!
    
    override func setUp() {
        super.setUp()
        mockProvider = MockSocketProvider(data: ["id": "test", "name": "Mock User"])
        app = Feathers(provider: mockProvider)
    }
    
    override func tearDown() {
        mockProvider = nil
        app = nil
        super.tearDown()
    }
    
    func testCorrectBaseProperties() {
        XCTAssertEqual(mockProvider.baseURL.absoluteString, "http://localhost:3030")
        XCTAssertTrue(mockProvider.supportsRealtimeEvents)
    }
    
    func testHandleServiceRequests() {
        let service = app.service(path: "users")
        let expectation = XCTestExpectation(description: "Service request should complete")
        
        service.request(.find(query: nil))
            .on(failed: { error in
                XCTFail("Request should not fail: \(error)")
            }, value: { response in
                XCTAssertNotNil(response.data)
                if case let .object(data) = response.data,
                   let dict = data as? [String: Any] {
                    XCTAssertEqual(dict["id"] as? String, "test")
                    XCTAssertEqual(dict["name"] as? String, "Mock User")
                }
                expectation.fulfill()
            })
            .start()
        
        wait(for: [expectation], timeout: 2.0)
    }
    
    func testHandleAuthentication() {
        let expectation = XCTestExpectation(description: "Authentication should complete")
        
        app.authenticate(["strategy": "local", "email": "test@example.com"])
            .on(failed: { error in
                XCTFail("Authentication should not fail: \(error)")
            }, value: { authData in
                XCTAssertEqual(authData["accessToken"] as? String, "mock_token")
                expectation.fulfill()
            })
            .start()
        
        wait(for: [expectation], timeout: 2.0)
    }
    
    func testHandleLogout() {
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
    
    func testHandleEventRegistration() {
        let eventSignal = mockProvider.on(event: "test-event")
        let expectation = XCTestExpectation(description: "Event should be received")
        
        eventSignal.observeValues { data in
            XCTAssertEqual(data["message"] as? String, "hello")
            expectation.fulfill()
        }
        
        // Simulate an event
        mockProvider.simulateEvent("test-event", data: ["message": "hello"])
        
        wait(for: [expectation], timeout: 1.0)
    }
}
