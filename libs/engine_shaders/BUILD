load("@rules_apple//apple:resources.bzl", "apple_metal_library")
load("@coreservices//buildenv:cc_rules.bzl", "mratedca_cc_binary", "mratedca_cc_library", "mratedca_cc_test")

apple_metal_library(
    name = "engine_shaders", 
    srcs = glob(["*.metal"]), 
    out = "MetalDbEngine.metal", 
    hdrs = glob(["*.h"]),
    visibility = ["//visibility:public"]
)

mratedca_cc_library(
    clang_tidy = False,
    name = "engine_shaders_headers",
    hdrs = glob(["*.h"]),
    includes = ["."],
    visibility = ["//visibility:public"]
)
