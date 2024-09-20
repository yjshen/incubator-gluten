
#[cxx::bridge]
mod ffi {
    extern "Rust" {
        /// Check if the given substrait plan can be offloaded to DP.
        fn can_offload_to_dp(substrait_bytes: &[u8]) -> bool;

        /// Compile the given substrait plan to a DFG and return the DFG instance ID.
        fn compile_substrait_to_dfg(substrait_bytes: &[u8]) -> u64;

        /// Evaluate the DFG with the given input and return the result.
        /// This can be used for simple operators such as filter, project,
        /// and also the probe phase of hash join.
        unsafe fn evaluate(dfg_instance_id: u64, input_ptr: *const u8) -> *const u8;

        /// Evaluate the DFG with the given input.
        /// This is used for complex operators such as sort build, aggregate hashtable build, etc.
        unsafe fn evaluate_build(dfg_instance_id: u64, input_ptr: *const u8);

        /// Evaluate the DFG with the given input.
        /// Unlike evaluate_build, this is mainly used for partial aggregate which doesn't need
        /// to build a full hash table and can output partial results.
        unsafe fn evaluate_partial_build(dfg_instance_id: u64, input_ptr: *const u8);

        /// Notify the DP runtime that there is no more input.
        /// So for instance, for sort operator, it could finish the sorting and
        /// we can call result_has_next and result_next to get the output.
        fn no_more_input(dfg_instance_id: u64);

        /// Check if there are more records to output.
        fn result_has_next(dfg_instance_id: u64) -> bool;

        /// Get the next bunch of records.
        fn result_next(dfg_instance_id: u64) -> *const u8;

        /// Close the DFG and release all resources.
        fn dfg_close(dfg_instance_id: u64);

        /// Get the current memory usage in bytes.
        fn memory_usage(dfg_instance_id: u64) -> u64;

        /// Trigger a spill and return the freed memory size in bytes.
        fn spill(dfg_instance_id: u64) -> u64;

        // MergeSource functions
        /// Compile the DFG for merge source.
        fn compile_dfg_merge_source(serialized_rel: &[u8]) -> u64;

        /// Get the next bunch of records from the merge source.
        fn dfg_merge_source_next(dfg_instance_id: u64) -> *const u8;

        /// Enqueue the input to the merge source.
        unsafe fn dfg_merge_source_enqueue(dfg_instance_id: u64, input_ptr: *const u8);

        /// Check if the merge source is full.
        fn dfg_merge_source_is_full(dfg_instance_id: u64) -> bool;

        /// Notify the DP runtime that there is no more input to the merge source.
        fn dfg_merge_source_no_more_input(dfg_instance_id: u64);

        /// Close the merge source and release all resources.
        fn dfg_merge_source_close(dfg_instance_id: u64);

        // MergeJoin functions
        /// Compile the DFG for merge join.
        fn compile_dfg_merge_join(serialized_rel: &[u8]) -> u64;

        /// Check if the merge join needs input.
        fn dfg_merge_join_needs_input(dfg_instance_id: u64) -> bool;

        /// Add the input to the merge join.
        unsafe fn dfg_merge_join_add_input(dfg_instance_id: u64, input_ptr: *const u8);

        /// Check if the merge join is blocked.
        fn dfg_merge_join_is_blocked(dfg_instance_id: u64) -> bool;

        /// Get the output from the merge join.
        fn dfg_merge_join_get_output(dfg_instance_id: u64) -> *const u8;

        /// Check if the merge join is finished.
        fn dfg_merge_join_is_finished(dfg_instance_id: u64) -> bool;

        /// Notify the DP runtime that there is no more input to the merge join.
        fn dfg_merge_join_no_more_input(dfg_instance_id: u64);

        /// Close the merge join and release all resources.
        fn dfg_merge_join_close(dfg_instance_id: u64);
    }

    unsafe extern "C++" {
        include!("bridge.h");
        include!("rust/cxx.h");

        #[allow(dead_code)]
        fn cpp_function(s: &CxxString) -> u32;
    }
}

/// Check if the given substrait plan can be offloaded to DP.
pub fn can_offload_to_dp(_substrait_bytes: &[u8]) -> bool {
    true
}

/// Compile the given substrait plan to a DFG and return the DFG instance ID.
pub fn compile_substrait_to_dfg(_substrait_bytes: &[u8]) -> u64 {
    1
}

/// Evaluate the DFG with the given input and return the result.
/// This can be used for simple operators such as filter, project,
/// and also the probe phase of hash join.
pub unsafe fn evaluate(_dfg_instance_id: u64, _input_ptr: *const u8) -> *const u8 {
    return _input_ptr;
}

/// Evaluate the DFG with the given input.
/// This is used for complex operators such as sort build, aggregate hashtable build, etc.
pub unsafe fn evaluate_build(_dfg_instance_id: u64, _input_ptr: *const u8) {
    todo!("Implement blocked build")
}

/// Evaluate the DFG with the given input.
/// This is used for partial aggregate which doesn't need
/// to build a full hash table and can output partial results.
pub unsafe fn evaluate_partial_build(_dfg_instance_id: u64, _input_ptr: *const u8) {
    todo!("Implement partial build")
}

/// Notify the DP runtime that there is no more input.
/// So for instance, for sort operator, it could finish the sorting and
/// we can call result_has_next and result_next to get the output.
pub fn no_more_input(_dfg_instance_id: u64) {
    todo!("Implement no more input")
}

/// Check if there are more records to output.
pub fn result_has_next(_dfg_instance_id: u64) -> bool {
    todo!("Implement result has next")
}

/// Get the next bunch of records.
pub fn result_next(_dfg_instance_id: u64) -> *const u8 {
    todo!("Implement result next")
}

/// Close the DFG and release all resources.
pub fn dfg_close(_dfg_instance_id: u64) {
    // TODO: Implement DFG close
}

/// Get the current memory usage in bytes.
pub fn memory_usage(_dfg_instance_id: u64) -> u64 {
    0
}

/// Trigger a spill and return the freed memory size in bytes.
pub fn spill(_dfg_instance_id: u64) -> u64 {
    0
}

/// Compile the DFG for merge source.
pub fn compile_dfg_merge_source(_serialized_rel: &[u8]) -> u64 {
    1
}

/// Get the next bunch of records from the merge source.
pub fn dfg_merge_source_next(_dfg_instance_id: u64) -> *const u8 {
    todo!("Implement merge source next")
}

/// Enqueue the input to the merge source.
pub unsafe fn dfg_merge_source_enqueue(_dfg_instance_id: u64, _input_ptr: *const u8) {
    todo!("Implement merge source enqueue")
}

/// Check if the merge source is full.
pub fn dfg_merge_source_is_full(_dfg_instance_id: u64) -> bool {
    todo!("Implement merge source is full")
}

/// Notify the DP runtime that there is no more input to the merge source.
pub fn dfg_merge_source_no_more_input(_dfg_instance_id: u64) {
    todo!("Implement merge source no more input")
}

/// Close the merge source and release all resources.
pub fn dfg_merge_source_close(_dfg_instance_id: u64) {
    todo!("Implement merge source close")
}

/// Compile the DFG for merge join.
pub fn compile_dfg_merge_join(_serialized_rel: &[u8]) -> u64 {
    1
}

/// Check if the merge join needs input.
pub fn dfg_merge_join_needs_input(_dfg_instance_id: u64) -> bool {
    todo!("Implement merge join needs input")
}

/// Add the input to the merge join.
pub unsafe fn dfg_merge_join_add_input(_dfg_instance_id: u64, _input_ptr: *const u8) {
    todo!("Implement merge join add input")
}

/// Check if the merge join is blocked.
pub fn dfg_merge_join_is_blocked(_dfg_instance_id: u64) -> bool {
    todo!("Implement merge join is blocked")
}

/// Get the output from the merge join.
pub fn dfg_merge_join_get_output(_dfg_instance_id: u64) -> *const u8 {
    todo!("Implement merge join get output")
}

/// Check if the merge join is finished.
pub fn dfg_merge_join_is_finished(_dfg_instance_id: u64) -> bool {
    todo!("Implement merge join is finished")
}

/// Notify the DP runtime that there is no more input to the merge join.
pub fn dfg_merge_join_no_more_input(_dfg_instance_id: u64) {
    todo!("Implement merge join no more input")
}

/// Close the merge join and release all resources.
pub fn dfg_merge_join_close(_dfg_instance_id: u64) {
    
}
