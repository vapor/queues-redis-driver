// swift-tools-version:4.2

import PackageDescription

let package = Package(
    name: "RedisJobs",
    products: [
        .library(
            name: "RedisJobs",
            targets: ["RedisJobs"]),
    ],
    dependencies: [
        .package(url: "https://github.com/vapor-community/jobs.git", from: "0.0.1"),
        .package(url: "https://github.com/vapor/redis.git", from: "3.0.0")
    ],
    targets: [
        .target(
            name: "RedisJobs",
            dependencies: ["Jobs", "Redis"]),
        .testTarget(
            name: "RedisJobsTests",
            dependencies: ["RedisJobs"]),
    ]
)
