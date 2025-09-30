#!/bin/bash

# Environment check script for Curvine build dependencies
# This script checks if all required dependencies are installed with minimum versions

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Track overall status
OVERALL_SUCCESS=true
FAILED_DEPENDENCIES=""

# Function to print status
print_status() {
    local status=$1
    local message=$2
    local dependency_name=$3
    if [ "$status" = "OK" ]; then
        echo -e "${GREEN}[✓]${NC} $message"
    elif [ "$status" = "WARN" ]; then
        echo -e "${YELLOW}[!]${NC} $message"
    else
        echo -e "${RED}[✗]${NC} $message"
        OVERALL_SUCCESS=false
        if [ -n "$dependency_name" ]; then
            FAILED_DEPENDENCIES="$FAILED_DEPENDENCIES $dependency_name"
        fi
    fi
}

# Function to compare version numbers
version_compare() {
    local version1=$1
    local version2=$2
    
    # Convert version strings to comparable format
    version1_num=$(echo "$version1" | sed 's/[^0-9.]//g' | awk -F. '{ printf("%d%03d%03d\n", $1,$2,$3); }')
    version2_num=$(echo "$version2" | sed 's/[^0-9.]//g' | awk -F. '{ printf("%d%03d%03d\n", $1,$2,$3); }')
    
    if [ "$version1_num" -ge "$version2_num" ]; then
        return 0
    else
        return 1
    fi
}

# Function to generate quick reference links for failed dependencies
generate_reference_links() {
    local failed_deps="$1"
    echo -e "${BLUE}Quick reference links for failed dependencies:${NC}"
    
    for dep in $failed_deps; do
        case $dep in
            "GCC")
                echo -e "  - ${YELLOW}GCC${NC}: ${GREEN}https://gcc.gnu.org/install/${NC}"
                ;;
            "RUST")
                echo -e "  - ${YELLOW}Rust${NC}: ${GREEN}https://rustup.rs/${NC}"
                ;;
            "PROTOBUF")
                echo -e "  - ${YELLOW}Protobuf${NC}: ${GREEN}https://grpc.io/docs/protoc-installation/${NC}"
                ;;
            "MAVEN")
                echo -e "  - ${YELLOW}Maven${NC}: ${GREEN}https://maven.apache.org/install.html${NC}"
                ;;
            "LLVM")
                echo -e "  - ${YELLOW}LLVM${NC}: ${GREEN}https://llvm.org/docs/GettingStarted.html${NC}"
                ;;
            "FUSE")
                echo -e "  - ${YELLOW}FUSE${NC}: Install libfuse2-dev or libfuse3-dev via your package manager"
                ;;
            "JDK")
                echo -e "  - ${YELLOW}JDK${NC}: ${GREEN}https://openjdk.java.net/install/${NC}"
                ;;
            "NPM")
                echo -e "  - ${YELLOW}npm${NC}: ${GREEN}https://nodejs.org/en/download/${NC}"
                ;;
            "PYTHON")
                echo -e "  - ${YELLOW}Python${NC}: ${GREEN}https://www.python.org/downloads/${NC}"
                ;;
        esac
    done
}

echo -e "${BLUE}=== Curvine Build Environment Check ===${NC}"
echo ""

# Check GCC (version 10 or later)
echo -e "${BLUE}Checking GCC...${NC}"
if command -v gcc >/dev/null 2>&1; then
    GCC_VERSION=$(gcc --version | head -n1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n1)
    if [ -z "$GCC_VERSION" ]; then
        GCC_VERSION=$(gcc --version | head -n1 | grep -oE '[0-9]+\.[0-9]+' | head -n1)
    fi
    if version_compare "$GCC_VERSION" "10.0.0"; then
        print_status "OK" "GCC $GCC_VERSION (>= 10.0.0 required)"
    else
        print_status "FAIL" "GCC $GCC_VERSION found, but version 10.0.0 or later is required" "GCC"
    fi
else
    print_status "FAIL" "GCC not found. Please install GCC version 10 or later" "GCC"
fi

# Check Rust (version 1.86 or later)
echo -e "${BLUE}Checking Rust...${NC}"
if command -v rustc >/dev/null 2>&1; then
    RUST_VERSION=$(rustc --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n1)
    if version_compare "$RUST_VERSION" "1.86.0"; then
        print_status "OK" "Rust $RUST_VERSION (>= 1.86.0 required)"
    else
        print_status "FAIL" "Rust $RUST_VERSION found, but version 1.86.0 or later is required" "RUST"
    fi
else
    print_status "FAIL" "Rust not found. Please install Rust version 1.86.0 or later" "RUST"
fi

# Check Protobuf (version 3.x+)
echo -e "${BLUE}Checking Protobuf...${NC}"
if command -v protoc >/dev/null 2>&1; then
    PROTOC_VERSION=$(protoc --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n1)
    if [ -z "$PROTOC_VERSION" ]; then
        PROTOC_VERSION=$(protoc --version | grep -oE '[0-9]+\.[0-9]+' | head -n1)
    fi
    if version_compare "$PROTOC_VERSION" "3.0.0"; then
        print_status "OK" "Protobuf $PROTOC_VERSION (>= 3.0.0 required)"
    else
        print_status "FAIL" "Protobuf $PROTOC_VERSION found, but version 3.0.0 or later is required" "PROTOBUF"
    fi
else
    print_status "FAIL" "Protobuf compiler (protoc) not found. Please install Protobuf version 3.0.0 or later" "PROTOBUF"
fi

# Check Maven (version 3.8 or later)
echo -e "${BLUE}Checking Maven...${NC}"
if command -v mvn >/dev/null 2>&1; then
    MAVEN_VERSION=$(mvn --version | head -n1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n1)
    if version_compare "$MAVEN_VERSION" "3.8.0"; then
        print_status "OK" "Maven $MAVEN_VERSION (>= 3.8.0 required)"
    else
        print_status "FAIL" "Maven $MAVEN_VERSION found, but version 3.8.0 or later is required" "MAVEN"
    fi
else
    print_status "FAIL" "Maven not found. Please install Maven version 3.8.0 or later" "MAVEN"
fi

# Check LLVM (version 12 or later)
echo -e "${BLUE}Checking LLVM...${NC}"
if command -v llvm-config >/dev/null 2>&1; then
    LLVM_VERSION=$(llvm-config --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n1)
    if [ -z "$LLVM_VERSION" ]; then
        LLVM_VERSION=$(llvm-config --version | grep -oE '[0-9]+\.[0-9]+' | head -n1)
    fi
    if version_compare "$LLVM_VERSION" "12.0.0"; then
        print_status "OK" "LLVM $LLVM_VERSION (>= 12.0.0 required)"
    else
        print_status "FAIL" "LLVM $LLVM_VERSION found, but version 12.0.0 or later is required" "LLVM"
    fi
elif command -v clang >/dev/null 2>&1; then
    CLANG_VERSION=$(clang --version | head -n1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n1)
    if [ -z "$CLANG_VERSION" ]; then
        CLANG_VERSION=$(clang --version | head -n1 | grep -oE '[0-9]+\.[0-9]+' | head -n1)
    fi
    if version_compare "$CLANG_VERSION" "12.0.0"; then
        print_status "OK" "LLVM/Clang $CLANG_VERSION (>= 12.0.0 required)"
    else
        print_status "FAIL" "LLVM/Clang $CLANG_VERSION found, but version 12.0.0 or later is required" "LLVM"
    fi
else
    print_status "FAIL" "LLVM not found. Please install LLVM version 12.0.0 or later" "LLVM"
fi

# Check FUSE development packages (optional)
echo -e "${BLUE}Checking FUSE...${NC}"
FUSE_FOUND=false
FUSE2_FOUND=false
FUSE3_FOUND=false
FUSE_FEATURE=""

# Check for libfuse3 development package
if pkg-config --exists fuse3 2>/dev/null; then
    FUSE3_VERSION=$(pkg-config --modversion fuse3 2>/dev/null || echo "unknown")
    print_status "OK" "libfuse3 development package found (version: $FUSE3_VERSION)"
    FUSE_FOUND=true
    FUSE3_FOUND=true
elif [ -f "/usr/include/fuse3/fuse.h" ] || [ -f "/usr/local/include/fuse3/fuse.h" ]; then
    print_status "OK" "libfuse3 development headers found"
    FUSE_FOUND=true
    FUSE3_FOUND=true
fi

# Check for libfuse2 development package
if pkg-config --exists fuse 2>/dev/null; then
    FUSE2_VERSION=$(pkg-config --modversion fuse 2>/dev/null || echo "unknown")
    print_status "OK" "libfuse2 development package found (version: $FUSE2_VERSION)"
    FUSE_FOUND=true
    FUSE2_FOUND=true
elif [ -f "/usr/include/fuse/fuse.h" ] || [ -f "/usr/local/include/fuse/fuse.h" ]; then
    print_status "OK" "libfuse2 development headers found"
    FUSE_FOUND=true
    FUSE2_FOUND=true
fi

# Determine FUSE feature to use
if [ "$FUSE3_FOUND" = true ] && [ "$FUSE2_FOUND" = true ]; then
    FUSE_FEATURE="fuse3"
    print_status "OK" "Both FUSE2 and FUSE3 found, will use FUSE3 for compilation"
elif [ "$FUSE3_FOUND" = true ]; then
    FUSE_FEATURE="fuse3"
    print_status "OK" "FUSE3 found, will use FUSE3 for compilation"
elif [ "$FUSE2_FOUND" = true ]; then
    FUSE_FEATURE="fuse2"
    print_status "OK" "FUSE2 found, will use FUSE2 for compilation"
else
    FUSE_FEATURE=""
    print_status "WARN" "FUSE development package not found. FUSE module will be skipped during compilation"
fi

# Check JDK (version 1.8 or later)
echo -e "${BLUE}Checking JDK...${NC}"
if command -v javac >/dev/null 2>&1; then
    JAVA_VERSION=$(javac -version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n1)
    if [ -z "$JAVA_VERSION" ]; then
        # Handle newer Java versions (9+) that use different versioning
        JAVA_VERSION=$(javac -version 2>&1 | grep -oE '[0-9]+' | head -n1)
        if [ -n "$JAVA_VERSION" ] && [ "$JAVA_VERSION" -ge 8 ]; then
            print_status "OK" "JDK $JAVA_VERSION (>= 1.8.0 required)"
        else
            print_status "FAIL" "JDK version could not be determined or is too old" "JDK"
        fi
    else
        if version_compare "$JAVA_VERSION" "1.8.0"; then
            print_status "OK" "JDK $JAVA_VERSION (>= 1.8.0 required)"
        else
            print_status "FAIL" "JDK $JAVA_VERSION found, but version 1.8.0 or later is required" "JDK"
        fi
    fi
else
    print_status "FAIL" "JDK not found. Please install JDK version 1.8.0 or later" "JDK"
fi

# Check npm (version 9 or later)
echo -e "${BLUE}Checking npm...${NC}"
if command -v npm >/dev/null 2>&1; then
    NPM_VERSION=$(npm --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n1)
    if version_compare "$NPM_VERSION" "9.0.0"; then
        print_status "OK" "npm $NPM_VERSION (>= 9.0.0 required)"
    else
        print_status "FAIL" "npm $NPM_VERSION found, but version 9.0.0 or later is required" "NPM"
    fi
else
    print_status "FAIL" "npm not found. Please install npm version 9.0.0 or later" "NPM"
fi

# Check Python (version 3.7 or later)
echo -e "${BLUE}Checking Python...${NC}"
if command -v python3 >/dev/null 2>&1; then
    PYTHON_VERSION=$(python3 --version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n1)
    if [ -z "$PYTHON_VERSION" ]; then
        PYTHON_VERSION=$(python3 --version 2>&1 | grep -oE '[0-9]+\.[0-9]+' | head -n1)
    fi
    if version_compare "$PYTHON_VERSION" "3.7.0"; then
        print_status "OK" "Python $PYTHON_VERSION (>= 3.7.0 required)"
    else
        print_status "FAIL" "Python $PYTHON_VERSION found, but version 3.7.0 or later is required" "PYTHON"
    fi
elif command -v python >/dev/null 2>&1; then
    # Check if 'python' command points to Python 3.7+
    PYTHON_VERSION=$(python --version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n1)
    if [ -z "$PYTHON_VERSION" ]; then
        PYTHON_VERSION=$(python --version 2>&1 | grep -oE '[0-9]+\.[0-9]+' | head -n1)
    fi
    # Check if it's Python 3.x
    PYTHON_MAJOR=$(echo "$PYTHON_VERSION" | cut -d. -f1)
    if [ "$PYTHON_MAJOR" = "3" ] && version_compare "$PYTHON_VERSION" "3.7.0"; then
        print_status "OK" "Python $PYTHON_VERSION (>= 3.7.0 required)"
    else
        print_status "FAIL" "Python $PYTHON_VERSION found, but version 3.7.0 or later is required" "PYTHON"
    fi
else
    print_status "FAIL" "Python not found. Please install Python version 3.7.0 or later" "PYTHON"
fi

echo ""
echo -e "${BLUE}=== Environment Check Summary ===${NC}"

if [ "$OVERALL_SUCCESS" = true ]; then
    echo -e "${GREEN}✓ All dependencies are satisfied!${NC}"
    echo "You can proceed with building Curvine."
    exit 0
else
    echo -e "${RED}✗ Some dependencies are missing or have insufficient versions.${NC}"
    echo "Please install or upgrade the missing dependencies before building."
    echo ""
    echo -e "${BLUE}📖 For detailed installation guides, please visit:${NC}"
    echo -e "${YELLOW}   https://curvineio.github.io/docs/Deploy/prerequisites${NC}"
    echo ""
    if [ -n "$FAILED_DEPENDENCIES" ]; then
        generate_reference_links "$FAILED_DEPENDENCIES"
    fi
    exit 1
fi