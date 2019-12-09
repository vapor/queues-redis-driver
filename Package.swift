// swift-tools-version:5.1
import PackageDescription

let package = Package(
    name: "jobs-redis-driver",
    platforms: [
       .macOS(.v10_14)
    ],
    products: [
        .library(
            name: "JobsRedisDriver",
            targets: ["JobsRedisDriver"]
        )
    ],
    dependencies: [
        .package(url: "https://github.com/vapor/vapor.git", from: "4.0.0-beta.2"),
        .package(url: "https://github.com/vapor/jobs.git", from: "1.0.0-beta.3"),
        .package(url: "https://github.com/vapor/redis-kit.git", from: "1.0.0-beta.2"),
    ],
    targets: [
        .target(
            name: "JobsRedisDriver",
            dependencies: ["Jobs", "RedisKit"]
        ),
        .testTarget(
            name: "JobsRedisDriverTests",
            dependencies: ["JobsRedisDriver", "XCTVapor"]
        ),
    ]
)
