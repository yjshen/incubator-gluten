use cxx_build;

fn main() {
    cxx_build::bridge("src/lib.rs")
        .file("../cpp/cpp_rust_interop/bridge.cc")
        .include("../cpp/cpp_rust_interop")
        .flag_if_supported("-std=c++14")
        .compile("dp_executor");

    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=../cpp/cpp_rust_interop/bridge.h");
    println!("cargo:rerun-if-changed=../cpp/cpp_rust_interop/bridge.cc");
}
