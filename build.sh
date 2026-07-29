#!/bin/bash
#
# Build script for Architect Platform Core
#
# Usage:
#   ./build.sh [release|debug|clean]
#
# Output:
#   bin/ax_bin/          - Executable binaries
#   bin/ax_bin/lib/      - Static/shared libraries
#   bin/make/            - CMake build files
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/bin/make"
OUTPUT_DIR="${SCRIPT_DIR}/bin/ax_bin"

# Default build type
BUILD_TYPE="${1:-release}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${BLUE}==========================================${NC}"
    echo -e "${BLUE}  Architect Platform Core - Build System${NC}"
    echo -e "${BLUE}==========================================${NC}"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Create directories if they don't exist
create_dirs() {
    mkdir -p "${BUILD_DIR}"
    mkdir -p "${OUTPUT_DIR}"
    mkdir -p "${OUTPUT_DIR}/lib"
}

# Clean build
clean_build() {
    print_info "Cleaning build directories..."
    rm -rf "${BUILD_DIR}"/*
    rm -rf "${OUTPUT_DIR}"/*
    print_success "Clean complete"
}

# Configure and build
do_build() {
    local cmake_build_type="Release"
    
    if [[ "${BUILD_TYPE}" == "debug" ]]; then
        cmake_build_type="Debug"
    elif [[ "${BUILD_TYPE}" == "relwithdebinfo" ]]; then
        cmake_build_type="RelWithDebInfo"
    fi
    
    print_info "Build type: ${cmake_build_type}"
    print_info "Build directory: ${BUILD_DIR}"
    print_info "Output directory: ${OUTPUT_DIR}"
    
    # Create directories
    create_dirs
    
    # Change to build directory
    cd "${BUILD_DIR}"
    
    # Configure
    print_info "Configuring CMake..."
    cmake ../.. -DCMAKE_BUILD_TYPE="${cmake_build_type}"
    
    # Detect number of CPUs (cross-platform)
    if command -v nproc &> /dev/null; then
        NUM_CPUS=$(nproc)
    elif command -v sysctl &> /dev/null; then
        NUM_CPUS=$(sysctl -n hw.ncpu)
    else
        NUM_CPUS=4
    fi
    
    # Build
    print_info "Building with ${NUM_CPUS} parallel jobs..."
    make -j${NUM_CPUS}
    
    print_success "Build complete!"
    echo ""
    print_info "Binaries located at: ${OUTPUT_DIR}"
    
    # List built executables
    if [[ -d "${OUTPUT_DIR}" ]]; then
        echo ""
        print_info "Built executables:"
        find "${OUTPUT_DIR}" -maxdepth 1 -type f -executable 2>/dev/null | while read -r exe; do
            echo "  - $(basename "${exe}")"
        done
    fi
}

# Main
print_header

case "${BUILD_TYPE}" in
    clean)
        clean_build
        ;;
    release|debug|relwithdebinfo)
        do_build
        ;;
    rebuild)
        clean_build
        BUILD_TYPE="release"
        do_build
        ;;
    *)
        echo "Usage: $0 [release|debug|relwithdebinfo|clean|rebuild]"
        echo ""
        echo "Options:"
        echo "  release        - Build with optimizations (default)"
        echo "  debug          - Build with debug symbols"
        echo "  relwithdebinfo - Build with optimizations and debug symbols"
        echo "  clean          - Remove all build artifacts"
        echo "  rebuild        - Clean and rebuild in release mode"
        exit 1
        ;;
esac

echo ""
echo -e "${BLUE}==========================================${NC}"
