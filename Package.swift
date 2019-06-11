// swift-tools-version:5.1

import PackageDescription

let package = Package(
    name: "JobsRedisDriver",
    products: [
        .library(
            name: "JobsRedisDriver",
            targets: ["JobsRedisDriver"]),
    ],
    dependencies: [
        .package(url: "https://github.com/vapor-community/jobs.git", from: "1.0.0-alpha.1.0"),
        .package(url: "https://github.com/vapor/redis-kit.git", .branch("master")),
        
    ],
    targets: [
        .target(
            name: "JobsRedisDriver",
            dependencies: ["Jobs", "RedisKit"]),
        .testTarget(
            name: "JobsRedisDriverTests",
            dependencies: ["JobsRedisDriver"]),
    ]
)
