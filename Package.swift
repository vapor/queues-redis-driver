// swift-tools-version:4.2

import PackageDescription

let package = Package(
    name: "JobsRedisDriver",
    products: [
        .library(
            name: "JobsRedisDriver",
            targets: ["JobsRedisDriver"]),
    ],
    dependencies: [
        .package(url: "https://github.com/vapor-community/jobs.git", .branch("master")),
        .package(url: "https://github.com/vapor/redis.git", from: "3.0.0")
    ],
    targets: [
        .target(
            name: "JobsRedisDriver",
            dependencies: ["Jobs", "Redis"]),
        .testTarget(
            name: "JobsRedisDriverTests",
            dependencies: ["JobsRedisDriver"]),
    ]
)
