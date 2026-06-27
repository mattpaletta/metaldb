load("@coreservices//buildenv:cc_rules.bzl", "mratedca_cc_binary", "mratedca_cc_library", "mratedca_cc_test")

mratedca_cc_binary(
    clang_tidy = False,
    name = "metaldb",
    srcs = glob(["**/*.cpp"]),
    deps = [
        "@metaldb//libs/engine"
    ]
)
