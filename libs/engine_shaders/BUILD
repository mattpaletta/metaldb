load("@rules_apple//apple:resources.bzl", "apple_metal_library")
load("@rules_cc//cc:defs.bzl", "cc_library")

apple_metal_library(
    name = "engine_shaders", 
    srcs = glob(["*.metal"]), 
    out = "MetalDbEngine.metal", 
    hdrs = glob(["*.h"]),
    visibility = ["//visibility:public"]
)

cc_library(
    name = "engine_shaders_headers",
    hdrs = glob(["*.h"]),
    includes = ["."],
    visibility = ["//visibility:public"]
)
