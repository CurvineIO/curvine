# Curvine Governance

Curvine is a [CNCF Sandbox](https://www.cncf.io/projects/curvine/) project. This document describes how the project is run: who makes decisions, how people take on more responsibility, and how changes get in.

All participants follow the [Code of Conduct](CODE_OF_CONDUCT.md).

## Roles

### Contributor

Anyone who opens an issue, sends a pull request, reviews code, improves documentation, or helps users is a contributor. No sign-up is needed.

### Committer

Committers have shown sustained, high-quality contributions in one or more areas. They:

- Review and approve pull requests in their areas
- Triage issues and help new contributors
- Vote on new committers and maintainers

### Maintainer

Maintainers are responsible for the project as a whole. In addition to committer duties, they:

- Set technical direction and maintain the [roadmap](ROADMAP.md)
- Cut releases and handle security reports (see [SECURITY.md](SECURITY.md))
- Manage repository settings, CI, and project infrastructure
- Represent the project in the CNCF

One maintainer serves as **lead maintainer** and coordinates releases and CNCF communication. The lead maintainer has no veto beyond a normal maintainer vote.

The current committers and maintainers are listed in [MAINTAINERS.md](MAINTAINERS.md).

## Becoming a committer or maintainer

1. A maintainer or committer nominates the candidate by opening a GitHub issue titled `New Committer Vote: <github-handle>` (or `New Maintainer Vote`). The issue summarizes the candidate's contributions with links: merged PRs, reviews, issues, and areas of work.
2. The vote runs through [GitVote](https://github.com/cncf/gitvote) using the settings in [`.gitvote.yml`](.gitvote.yml): voting is open for 30 days, and the vote passes when at least 60% of binding votes are in favor. Votes may close early once the threshold is reached.
3. Binding votes come from members of the `curvine-toc-voters` team and the users listed in `.gitvote.yml`.
4. After the vote passes, a maintainer updates MAINTAINERS.md and grants repository permissions.

There is no fixed contribution count. Voters look for sustained work over several months, sound technical judgment, helpful code reviews, and respectful collaboration.

## Decision making

Most decisions are made through normal pull request review:

- Every change to `main` goes through a pull request. Direct pushes are blocked.
- A pull request needs at least **one approving review** from a committer or maintainer other than the author, and passing CI.
- Pull requests are merged with squash or rebase to keep a linear history.

Larger changes need wider agreement:

| Change | Process |
| --- | --- |
| New features, protocol or on-disk format changes, breaking API changes | Open an issue describing the design first. Wait for maintainer feedback before large implementation work. |
| Changes to this document, the license, or the Code of Conduct | GitVote among maintainers, 60% in favor. |
| Adding or removing committers and maintainers | GitVote as described above. |

We aim for consensus. When consensus cannot be reached, any maintainer can call a GitVote to decide.

## Stepping down and inactivity

Committers and maintainers can step down at any time by opening a pull request against MAINTAINERS.md. Those who step down are listed as emeritus.

A committer or maintainer who has been inactive for 12 months may be moved to emeritus status by a maintainer vote, after being contacted first.

## Vendor neutrality

Curvine welcomes contributors and maintainers from any company. Decisions are made on technical merit in public: in GitHub issues, pull requests, and community channels. We aim to grow the maintainer group so that no single company holds a majority of maintainer seats.

## Communication

- GitHub Issues and Pull Requests: design and code discussion
- [GitHub Discussions](https://github.com/CurvineIO/curvine/discussions): questions and ideas
- [Slack](https://join.slack.com/t/curvineio/shared_invite/zt-4673r43cn-prajma_q5ZI3BUxuaY5kiQ) and [Discord](https://discord.gg/xfxMQKv3): real-time chat
- Security issues: follow [SECURITY.md](SECURITY.md); do not open public issues

## Changes to this document

Changes to this document are proposed by pull request and need a passing maintainer GitVote.
