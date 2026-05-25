// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "FeathersSwiftSocketIO",
    platforms: [
        .iOS(.v18),
        .macOS(.v15),
        .watchOS(.v11),
        .visionOS(.v2)
    ],
    products: [
        .library(
            name: "FeathersSwiftSocketIO",
            targets: ["FeathersSwiftSocketIO"]),
    ],
    dependencies: [
        .package(url: "https://github.com/socketio/socket.io-client-swift", from: "16.0.0"),
        .package(url: "https://github.com/ReactiveCocoa/ReactiveSwift", from: "7.0.0"),
        .package(path: "../feathers-swift"),
    ],
    targets: [
        .target(
            name: "FeathersSwiftSocketIO",
            dependencies: [
                .product(name: "SocketIO", package: "socket.io-client-swift"),
                .product(name: "ReactiveSwift", package: "ReactiveSwift"),
                .product(name: "Feathers", package: "feathers-swift"),
            ],
            path: "FeathersSwiftSocketIO/Core"
        ),
        .testTarget(
            name: "FeathersSwiftSocketIOTests",
            dependencies: [
                "FeathersSwiftSocketIO",
                .product(name: "Feathers", package: "feathers-swift"),
                .product(name: "SocketIO", package: "socket.io-client-swift"),
            ],
            path: "Tests"
        ),
    ]
)
