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

## AI-Assisted Contribution Workflow

Curvine keeps a set of versioned AI agent skills under [`.agents/skills/`](.agents/skills/), all prefixed with `cv-`. They encode the project's issue, implementation, pull request, and review conventions so an AI coding agent can perform the routine steps consistently. Using them is optional, but they are the canonical checklists for the workflows described above, even if you contribute manually.

### How skills are delivered

- `.agents/skills/<name>/SKILL.md` is the canonical source. `.claude/skills/`, `.cursor/skills/`, and `.github/skills/` are symlinks to it.
- Invoke a skill by name where slash commands are supported (for example `/cv-create-issue` in Claude Code or GitHub Copilot); Cursor discovers the same skills automatically.
- Skills never create issues, push commits, open pull requests, or post review comments without your explicit confirmation. They draft first and act after you approve.
- All GitHub-facing text produced by a skill is in English and follows [`.agents/rules/workflow.md`](.agents/rules/workflow.md).

### Skill map

| Contribution action | Skill | What it does |
| ------------------- | ----- | ------------ |
| Create an issue | `cv-create-issue` | Searches for duplicates, picks the bug or feature template, drafts the body (features also get Goal / Not Goal / Test Plan), then files the issue. |
| Resolve an issue | `cv-handle-issue` | Reads the issue, summarizes impact and risks, proposes a plan, waits for your approval, creates the branch, implements with tests, then hands off to PR creation. |
| Break down a large change | `cv-tasks-breakdown` | Splits work estimated above ~500 lines into dependency-ordered, commit-sized tasks with tests, and creates `[SUB]` sub-issues. |
| Submit a pull request | `cv-create-pr` | Runs `make format`, reviews the diff, commits, and drafts the PR title and body using the required sections. |
| Review a pull request | `cv-submit-pr-review` | Reviews the diff against the live base and head: intent, tests first, dirty crates, lifecycle, contracts, compatibility, and critical-path performance. |
| Handle review comments | `cv-address-pr-review` | Collects every comment, produces an accept/reject table for your approval, applies fixes, replies, and resolves the fixed threads. |
| Explore the codebase | `cv-codegraph` | Uses the local CodeGraph index for symbol lookup and call-path tracing; falls back to grep and direct reads when it is unavailable. |
| Test and CI | `cv-csi-test`, `cv-run-workflow`, `cv-test-report` | CSI integration scenarios, manual GitHub Actions dispatch, and publishing full-chain test reports. |
| Add or update a skill | `cv-add-skills` | Documents the naming, layout, and `AGENTS.md` registry rules for new `cv-*` skills. |

### 1. Create an issue

Describe what you observed and ask the agent to file it:

```text
/cv-create-issue
FUSE read of a 64 MiB file hangs after a worker restart; client version ...
```

The skill first searches open issues for duplicates, then selects `.github/ISSUE_TEMPLATE/bug_report.md` or `feature_request.md` and drafts the body. Bug reports require reproduction steps and environment details. Feature requests additionally require **Goal**, **Not Goal**, and **Test Plan** sections. Nothing is created until you review and approve the draft. Sub-issues of a parent issue use the `[SUB]` title prefix and link `Parent: #<number>`.

### 2. Resolve an issue

Give the agent the issue number or URL:

```text
/cv-handle-issue 123
```

The flow is: read the issue → summarize the problem, affected modules, risks, and test strategy → propose a plan → **wait for your approval** → create a `fix/123-<slug>` or `feat/123-<slug>` branch → implement the approved scope with tests → run `make format` → hand off to `cv-create-pr`. No code is written before the plan is approved.

If the estimated change exceeds ~500 lines or spans several modules, the skill routes to `cv-tasks-breakdown`, which produces a dependency-ordered task table plus sub-issues; each task should land as one commit and leave the build and tests green.

### 3. Submit a pull request

When your commits are ready:

```text
/cv-create-pr
```

The skill runs `make format`, checks the diff, and drafts:

- **Title**: `<type>(<module>): <description> (#<issueNumber>)`
- **Body**: Summary, Issue Describe / Design, Changes table, Test verified table, Dependencies

PRs are created as drafts unless you ask for a ready PR. Record the exact tests you ran in **Test verified**; a green CI job alone is not a verification story. Commit messages follow [COMMIT_CONVENTION.md](COMMIT_CONVENTION.md) and should reference the related issue as `(#number)`.

### 4. Review code

Two skills cover the two sides of code review:

- **As a reviewer**, run `/cv-submit-pr-review <PR-number>`. The skill verifies the live base and head, reads changed tests before the implementation, maps the dirty crates, and checks correctness, concurrency and lifecycle, wire compatibility, and performance on data I/O, RPC, and metadata paths. It drafts comments with `Critical:` / `Required:` / `Optional:` prefixes plus a local verdict, and posts only after you confirm.
- **As an author**, run `/cv-address-pr-review <PR-number>`. The skill reads every review comment, presents an accept / partial / reject table with reasons, applies the approved fixes, pushes the update, replies to each comment in English, and resolves the fixed threads.

### Ground rules

- You own the contribution: review everything the agent writes, run the tests yourself, and understand the change before submitting it.
- Skills are guidance, not enforcement. CI still validates formatting, compilation, tests, and the PR title.
- Skills must be written in English. Add or change one only through `cv-add-skills`, and keep the registry in [`AGENTS.md`](AGENTS.md) in sync.

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

If you use an AI coding agent, the [`cv-*` skills](#ai-assisted-contribution-workflow) can automate these steps and keep the issue, PR, and review formats consistent. You remain responsible for reviewing the output and verifying the change.

We appreciate all contributions, big and small. Thank you for helping make Curvine better!

---

For more detailed information about specific areas of contribution, see:
- [Development Guide](https://curvineio.github.io/docs/Contribute/development-guide)
- [Architecture Guide](https://curvineio.github.io/docs/Architecture/instrodction)
- [Testing Guide](https://curvineio.github.io/docs/Contribute/benchmark-guide)
- [Deployment Guide](https://curvineio.github.io/docs/Deploy/quick-start)