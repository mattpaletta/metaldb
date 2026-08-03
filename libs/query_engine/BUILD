load("@coreservices//buildenv:cc_rules.bzl", "mratedca_cc_binary", "mratedca_cc_library", "mratedca_cc_test")

mratedca_cc_library(
    clang_tidy = False,
    name = "query_engine",
    srcs = glob(["src/**/*.cpp"]),
    hdrs = glob(["include/**/*.hpp"]),
    includes = ["include"],
    deps = [
        "@metaldb//libs/engine_shaders:engine_shaders_headers"
    ],
    visibility = ["//visibility:public"],
    copts = ["-std=c++20"]
)
