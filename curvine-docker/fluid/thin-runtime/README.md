# Curvine Fluid ThinRuntime Docker Image

This directory contains the Docker build configuration for Curvine Fluid ThinRuntime, providing a containerized solution for distributed file system management.

## Build Modes

The build system supports two modes:

### 1. Pre-built Binary Mode (Recommended)
- Uses pre-compiled binaries from the `build/` directory
- Faster build process
- Smaller final image size
- Uses single-stage Docker build

### 2. Local Workspace Build Mode
- Builds from complete source code in the `workspace/` directory
- Includes full development toolchain
- Uses multi-stage Docker build
- Longer build time but ensures latest code

## Quick Start

Run the interactive build script:

```bash
./build-image.sh
```

The script will present you with build options:
1. **Build from pre-built binaries (Recommended)** - Fast build using existing artifacts
2. **Build from local workspace** - Complete source build with full toolchain
3. **Exit** - Cancel the build process

## Build Optimization Features

### Alibaba Cloud Mirror
To improve package download speeds in China, both Docker images are configured to use Alibaba Cloud mirrors:
- Base images use `mirrors.aliyun.com` for Ubuntu packages
- Rust toolchain uses Alibaba Cloud Rustup mirror
- Maven uses Alibaba Cloud repository mirror

### Docker Build Context
The build process automatically optimizes the Docker build context:
- Only necessary files are included
- Large directories are excluded to reduce build time
- Different contexts for different build modes

## Files Structure

```
fluid/thin-runtime/
├── build-image.sh          # Interactive build script
├── Dockerfile              # Multi-stage build for workspace mode
├── Dockerfile.binary       # Single-stage build for binary mode
├── fluid-config-parse.py   # Configuration parser
├── README.md               # This documentation
├── build/                  # Pre-built binaries directory
├── workspace/              # Complete source code directory
└── compile/                # Build configuration files
    ├── config              # Rust cargo configuration
    └── settings.xml        # Maven settings
```

## Environment Variables

The Docker images support the following environment variables:

- `CURVINE_HOME`: Base directory for Curvine installation (default: `/opt/curvine`)
- `ORPC_BIND_HOSTNAME`: Hostname for RPC binding (default: `0.0.0.0`)
- `CURVINE_CONF_FILE`: Path to configuration file (default: `/opt/curvine/conf/curvine-cluster.toml`)
- `BUILD_FROM_SOURCE`: Build mode flag (true/false)

## Manual Build Commands

If you prefer to build manually without the interactive script:

### Pre-built Binary Mode
```bash
docker build -f Dockerfile.binary -t curvine/fluid-thin-runtime:binary .
```

### Local Workspace Mode
```bash
docker build --build-arg BUILD_FROM_SOURCE=true -t curvine/fluid-thin-runtime:workspace .
```

## Troubleshooting

### Slow Package Downloads
If you experience slow package downloads, the images are already configured with Alibaba Cloud mirrors. If issues persist:
1. Check your network connection
2. Consider using a VPN if accessing from outside China
3. Verify the mirror configuration is working

### Build Failures
1. Ensure you have the necessary directories (`build/` or `workspace/`)
2. Check that all required files are present
3. Verify Docker has sufficient resources
4. Check the build logs for specific error messages

### Permission Issues
If you encounter permission issues:
```bash
chmod +x build-image.sh
./build-image.sh
```

## Contributing

When contributing to this build system:
1. Test both build modes
2. Ensure the interactive script works correctly
3. Update documentation for any new features
4. Maintain compatibility with existing configurations

## License

This project follows the same license as the main Curvine project.
