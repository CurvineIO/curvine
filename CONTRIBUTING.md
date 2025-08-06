# Contributing to Curvine

We welcome contributions to Curvine in many areas, and encourage new contributors to get involved. We believe people like you would make Curvine a great product.

Here are some areas where you can help:

* Testing Curvine with existing distributed cache system operations and reporting issues for any bugs or performance issues
* Contributing code to support additional storage backends, file system features, and data types that are not currently supported
* Contribute code to support data read/write acceleration in multiple scenarios such as AI, big data, and multi-cloud, while being compatible with various computing engines and AI infrastructure
* Reviewing pull requests and helping to test new features for correctness and performance
* Improving documentation and examples
* Adding new language bindings and client libraries
* Enhancing monitoring, metrics, and observability features

## Finding issues to work on

We maintain a list of good first issues in GitHub here. Look for issues labeled with:
- `good first issue` - Suitable for new contributors
- `help wanted` - Issues that need community help
- `bug` - Bug reports that need fixing
- `enhancement` - Feature requests and improvements

## Reporting issues

We use GitHub issues for bug reports and feature requests. When reporting an issue, please include:

### For Bug Reports:
- **Description**: Clear description of the bug
- **Steps to reproduce**: Detailed steps to reproduce the issue
- **Expected behavior**: What you expected to happen
- **Actual behavior**: What actually happened
- **Environment**: OS, Curvine version, configuration details
- **Logs**: Relevant error messages and logs
- **Screenshots**: If applicable

### For Feature Requests:
- **Description**: Clear description of the feature
- **Use case**: Why this feature would be useful
- **Proposed solution**: Your ideas for implementation (optional)

## Development Setup

### Prerequisites
- Rust 1.86+ 
- NPM 9+ (for web UI development)
- Maven 3.8+ (for Java client)
- JDK: 1.8+
- Protobuf 3.x
- LLVM 12+ 
- FUSE libfuse2 or libfuse3 development packages
- Python 3.7+

### Building from Source
```bash
# Clone the repository
git clone https://github.com/CurvineIO/curvine.git
cd curvine

# Build the project
./build/make all

# The built artifacts will be in build/dist/
```

### Running Tests
```bash
# Run Rust tests
cargo test

# Run integration tests
cargo test --test integration_tests

# Run benchmarks
cargo bench
```

## Code Style and Guidelines

### Rust Code
- Follow Rust coding conventions
- Use `cargo fmt` to format code
- Use `cargo clippy` for linting
- Write unit tests for new functionality
- Add integration tests for complex features

### Commit Messages
- Use conventional commit format
- Start with type: `feat:`, `fix:`, `docs:`, `style:`, `refactor:`, `test:`, `chore:`
- Provide clear, descriptive commit messages
- Reference issue numbers when applicable
- [Commit convention](https://github.com/CurvineIO/curvine/blob/main/COMMIT_CONVENTION.md)

### Pull Request Guidelines
- Create feature branches from `main`
- Keep PRs focused and reasonably sized
- Include tests for new functionality
- Update documentation as needed
- Ensure all CI checks pass

## Architecture Overview

Curvine follows a distributed architecture with the following components:

- **Master Node**: Manages metadata and coordinates operations
- **Worker Nodes**: Handle data storage and processing
- **Client Libraries**: Provide APIs for different languages
- **Web UI**: Management interface for monitoring and administration

## Testing Guidelines

### Unit Tests
- Write tests for all new functionality
- Aim for high test coverage
- Use meaningful test names and descriptions

### Integration Tests
- Test component interactions
- Verify end-to-end workflows
- Test error conditions and edge cases

### Performance Tests
- Benchmark critical paths
- Monitor for performance regressions
- Test with realistic data sizes

## Documentation

### Code Documentation
- Document public APIs with Rust doc comments
- Include usage examples
- Explain complex algorithms and design decisions

### User Documentation
- Update README.md for new features
- Add configuration examples
- Document troubleshooting steps

## Asking for Help

- **GitHub Discussions**: Use GitHub Discussions for general questions
- **GitHub Issues**: For bug reports and feature requests
- **[Discord](https://discord.gg/xfxMQKv3)**: Join our community channels for real-time discussions
- **Email**: [curvine86@gmail.com] Contact the maintainers for sensitive issues

## Regular Community Meetings

The Curvine contributors hold regular video calls where new and current contributors are welcome to ask questions and coordinate on issues they are working on.

Meeting details and schedule will be announced through:
- GitHub Discussions
- Project mailing list
- Community chat channels

## Code of Conduct

This project follows the [Apache Software Foundation Code of Conduct](https://www.apache.org/foundation/policies/conduct.html). Please be respectful and inclusive in all interactions.

## License

By contributing to Curvine, you agree that your contributions will be licensed under the Apache License 2.0.

## Getting Started for New Contributors

1. **Fork the repository** on GitHub
2. **Clone your fork** locally
3. **Set up the development environment** following the setup instructions
4. **Pick an issue** labeled with `good first issue`
5. **Create a feature branch** for your work
6. **Make your changes** following the coding guidelines
7. **Write tests** for your changes
8. **Submit a pull request** with a clear description

We appreciate all contributions, big and small. Thank you for helping make Curvine better!

---

For more detailed information about specific areas of contribution, see:
- [Development Guide](https://curvineio.github.io/docs/Contribute/development-guide)
- [Architecture Guide](https://curvineio.github.io/docs/Architecture/instrodction)
- [Testing Guide](https://curvineio.github.io/docs/Contribute/benchmark-guide)
- [Deployment Guide](https://curvineio.github.io/docs/Deploy/quick-start)