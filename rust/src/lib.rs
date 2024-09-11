use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use thiserror::Error;

pub struct Rel {}

impl Rel {
    pub fn decode(_bytes: Bytes) -> Result<Self, ExecutorError> {
        todo!("Implement decoding from bytes")
    }
}

#[derive(Error, Debug)]
pub enum ExecutorError {
    #[error("Failed to decode Substrait plan")]
    DecodingError(#[from] prost::DecodeError),
    #[error("No relations found in the plan")]
    NoRelationsError,
    #[error("Unsupported relation type")]
    UnsupportedRelationType,
    // Add more error types as needed
}

pub struct Executor {
    // Add fields as needed to represent the execution plan
    pub rel_node: Rel,
}

impl Executor {
    pub fn new(_rel_node: &Rel) -> Result<Self, ExecutorError> {
        // Construct the executor from the RelNode
        todo!("Implement executor construction from RelNode")
    }

    pub fn process(&self, _input: &RecordBatch) -> Result<RecordBatch, ExecutorError> {
        // Process the input RecordBatch and produce an output RecordBatch
        todo!("Implement execution logic")
    }
}


#[cxx::bridge]
mod ffi {
    extern "Rust" {
        fn can_offload_to_dp(bytes: &[u8]) -> bool;
    }

    unsafe extern "C++" {
        include!("bridge.h");
        include!("rust/cxx.h");

        #[allow(dead_code)]
        fn cpp_function(s: &CxxString) -> u32;
    }
}

pub fn can_offload_to_dp(_bytes: &[u8]) -> bool {
    // match Rel::decode(Bytes::copy_from_slice(bytes)) {
    //     Ok(rel) => {
    //         let convert_res = try_convert_to_qflow(&rel);
    //         convert_res.is_ok() && convert_res.unwrap().is_some()
    //     }
    //     Err(_) => false,
    // }
    true // always offload for now
}


pub struct QFlow {}

pub fn try_convert_to_qflow(_rel: &Rel) -> Result<Option<QFlow>, ExecutorError> {
    Ok(Some(QFlow {}))
}
