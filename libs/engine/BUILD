load("@rules_cc//cc:defs.bzl", "objc_library")
load("@cpptest//:cpptest.bzl", "cpptest_create")
load("@coreservices//buildenv:compatibility.bzl", "apple_host_only")

objc_library(
    name = "engine",
    srcs = glob(["src/**/*.cpp", "src/**/*.mm", "src/**/*.hpp"], exclude = ["src/**/*.test.cpp"]),
    hdrs = glob(["include/**/*.hpp"]),
    includes = ["include"],
    linkopts = [
        "-framework Foundation",
        "-framework Metal",
        "-framework MetalKit"
    ],
    deps = [
        "@metaldb//libs/query_engine",
        "@metaldb//libs/reader",
        "@metaldb//libs/engine_shaders:engine_shaders_headers",
        "@taskflow"
    ],
    data = ["@metaldb//libs/engine_shaders"],
    visibility = ["//visibility:public"],
    copts = ["-std=c++20"],
    target_compatible_with = apple_host_only(),
)

cpptest_create(
    name = "engine",
    srcs = glob(["src/**/*.test.cpp", "src/**/*.test.hpp"]),
)
