add_library(cpp_rust_interop STATIC
    bridge.cc
)

target_include_directories(cpp_rust_interop 
    PUBLIC 
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${RUST_GENERATED_INCLUDE}
)

# Link against the Rust library
target_link_libraries(cpp_rust_interop PRIVATE ${RUST_LIB_PATH})

# Make sure the Rust library and headers are built before this target
add_dependencies(cpp_rust_interop ensure_rust_lib)
