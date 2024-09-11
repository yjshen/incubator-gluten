#pragma once

#include <cstdint>
#include <vector>
#include <string>
#include "rust/cxx.h"
#include "dp_executor/src/lib.rs.h"

uint32_t cpp_function(const std::string& s);

bool can_offload_to_dp_cpp(const std::vector<uint8_t>& serializedFilter);
