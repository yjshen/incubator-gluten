#!/bin/bash
set -exu

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
GLUTEN_DIR="$CURRENT_DIR/.."
BUILD_TYPE=Release
BUILD_TESTS=OFF
BUILD_EXAMPLES=OFF
BUILD_BENCHMARKS=OFF
BUILD_JEMALLOC=OFF
BUILD_PROTOBUF=OFF
ENABLE_QAT=OFF
ENABLE_IAA=OFF
ENABLE_HBM=OFF
ENABLE_GCS=OFF
ENABLE_S3=OFF
ENABLE_HDFS=OFF
ENABLE_ABFS=OFF

# Set default number of threads as CPU cores minus 2
if [[ "$(uname)" == "Darwin" ]]; then
    physical_cpu_cores=$(sysctl -n hw.physicalcpu)
    ignore_cores=2
    if [ "$physical_cpu_cores" -gt "$ignore_cores" ]; then
        NUM_THREADS=${NUM_THREADS:-$(($physical_cpu_cores - $ignore_cores))}
    else
        NUM_THREADS=${NUM_THREADS:-$physical_cpu_cores}
    fi
else
    NUM_THREADS=${NUM_THREADS:-$(nproc --ignore=2)}
fi

# Parse command line arguments
for arg in "$@"
do
    case $arg in
        --build_type=*)
        BUILD_TYPE="${arg#*=}"
        shift
        ;;
        --build_tests=*)
        BUILD_TESTS="${arg#*=}"
        shift
        ;;
        --build_examples=*)
        BUILD_EXAMPLES="${arg#*=}"
        shift
        ;;
        --build_benchmarks=*)
        BUILD_BENCHMARKS="${arg#*=}"
        shift
        ;;
        --build_jemalloc=*)
        BUILD_JEMALLOC="${arg#*=}"
        shift
        ;;
        --enable_qat=*)
        ENABLE_QAT="${arg#*=}"
        shift
        ;;
        --enable_iaa=*)
        ENABLE_IAA="${arg#*=}"
        shift
        ;;
        --enable_hbm=*)
        ENABLE_HBM="${arg#*=}"
        shift
        ;;
        --build_protobuf=*)
        BUILD_PROTOBUF="${arg#*=}"
        shift
        ;;
        --enable_gcs=*)
        ENABLE_GCS="${arg#*=}"
        shift
        ;;
        --enable_s3=*)
        ENABLE_S3="${arg#*=}"
        shift
        ;;
        --enable_hdfs=*)
        ENABLE_HDFS="${arg#*=}"
        shift
        ;;
        --enable_abfs=*)
        ENABLE_ABFS="${arg#*=}"
        shift
        ;;
        --num_threads=*)
        NUM_THREADS="${arg#*=}"
        shift
        ;;
    esac
done

function build_gluten_cpp {
  echo "Start to build Gluten CPP"
  cd $GLUTEN_DIR/cpp
  rm -rf build
  mkdir build
  cd build
  cmake -DBUILD_VELOX_BACKEND=ON -DCMAKE_BUILD_TYPE=$BUILD_TYPE \
        -DBUILD_TESTS=$BUILD_TESTS -DBUILD_EXAMPLES=$BUILD_EXAMPLES -DBUILD_BENCHMARKS=$BUILD_BENCHMARKS -DBUILD_JEMALLOC=$BUILD_JEMALLOC \
        -DENABLE_HBM=$ENABLE_HBM -DENABLE_QAT=$ENABLE_QAT -DENABLE_IAA=$ENABLE_IAA -DBUILD_PROTOBUF=$BUILD_PROTOBUF -DENABLE_GCS=$ENABLE_GCS \
	-DCMAKE_CXX_FLAGS="-Wno-unused-variable -Wno-unused-private-field" \
        -DENABLE_S3=$ENABLE_S3 -DENABLE_HDFS=$ENABLE_HDFS -DENABLE_ABFS=$ENABLE_ABFS ..
  make -j $NUM_THREADS
}

# Run the build function
build_gluten_cpp
