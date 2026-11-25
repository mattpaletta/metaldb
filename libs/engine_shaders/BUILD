load("@rules_apple//apple:resources.bzl", "apple_metal_library")
load("@coreservices//buildenv:cc_rules.bzl", "mratedca_cc_binary", "mratedca_cc_library", "mratedca_cc_test")
load("@coreservices//buildenv:compatibility.bzl", "apple_host_only")

apple_metal_library(
    name = "engine_shaders", 
    srcs = glob(["*.metal"]), 
    out = "MetalDbEngine.metal", 
    hdrs = glob(["*.h"]),
    # The Metal compiler is Xcode's: elsewhere this target is skipped, not failed.
    target_compatible_with = apple_host_only(),
    visibility = ["//visibility:public"]
)

mratedca_cc_library(
    # Not yet formatted to the central style (coreservices .clang-format).
    clang_tidy = False,
    format = False,
    name = "engine_shaders_headers",
    hdrs = glob(["*.h"]),
    includes = ["."],
    visibility = ["//visibility:public"]
)
