add_library(cpp_rust_interop STATIC
    bridge.cc
)

target_include_directories(cpp_rust_interop 
    PUBLIC 
        ${CMAKE_CURRENT_SOURCE_DIR}
)
