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
        .package(url: "https://github.com/vapor/vapor.git", .branch("master")),
        .package(url: "https://github.com/vapor/jobs.git", .branch("master")),
        .package(url: "https://github.com/vapor/redis-kit.git", .branch("master")),
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
