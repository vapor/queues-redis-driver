// swift-tools-version:4.2

import PackageDescription

let package = Package(
    name: "RedisJobs",
    dependencies: [
        .package(url: "https://github.com/vapor-community/jobs.git", .branch("master"))
    ],
    targets: [
        .target(
            name: "RedisJobs",
            dependencies: ["Jobs"]),
        .testTarget(
            name: "RedisJobsTests",
            dependencies: ["RedisJobs"]),
    ]
)
