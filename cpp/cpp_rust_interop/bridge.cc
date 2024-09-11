#include "bridge.h"
#include <vector>
#include <iostream>

uint32_t cpp_function(const std::string& s) {
    return s.length();
}

bool can_offload_to_dp_cpp(const std::vector<uint8_t>& serializedFilter) {
  return can_offload_to_dp(rust::Slice<const uint8_t>(serializedFilter.data(), serializedFilter.size()));
}

void dummy_calling_rust() {
  std::vector<uint8_t> bytes = {1, 2, 3, 4, 5};

  // Call Rust function directly with rust::Slice
  bool can_offload = can_offload_to_dp(rust::Slice<const uint8_t>(bytes.data(), bytes.size()));

  // Use the result
  if (can_offload) {
    std::cout << "Can offload to DP" << std::endl;
  } else {
    std::cout << "Cannot offload to DP" << std::endl;
  }
}
