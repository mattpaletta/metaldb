load("@rules_cc//cc:defs.bzl", "cc_library")

cc_library(
    name = "reader",
    srcs = glob(["src/**/*.cpp"]),
    hdrs = glob(["include/**/*.hpp"]),
    includes = ["include"],
    deps = [
        "@cppnotstdlib"
        # "@metaldb//libs/engine_shaders:engine_shaders_headers"
    ],
    visibility = ["//visibility:public"],
    copts = ["-std=c++20"]
)
