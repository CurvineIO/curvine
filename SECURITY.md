# Security Policy

## Supported Versions

Curvine is pre-1.0 and releases move quickly. Security fixes are made on `main`
and shipped in the next release of the latest minor line. Older minor lines do
not receive backports.

| Version | Supported |
| ------- | --------- |
| 0.5.x (latest release) | :white_check_mark: |
| < 0.5 | :x: |

If you run an older version, upgrade to the latest release to get security fixes.

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues,
discussions, or pull requests.**

### Report through GitHub (preferred)

Open a private report at
<https://github.com/CurvineIO/curvine/security/advisories/new>
(the **Report a vulnerability** button on the repository's **Security** tab).
Only the maintainers can see the report, and the discussion and fix can happen
in a private fork.

### Report by email

If you cannot use GitHub, email **curvine86@gmail.com**.

### What to include

- A description of the vulnerability
- Steps to reproduce the issue
- Affected versions
- Potential impact
- Any suggested fixes (if available)

## Response Timeline

- We acknowledge your report within **5 business days**.
- We send a detailed response within **15 business days**, with the next steps
  for handling the report.
- We keep you informed of progress towards a fix and may ask for more
  information or guidance.

## Disclosure and Security Updates

- Fixes are released as soon as possible after they are available.
- Each fixed vulnerability is published as a
  [GitHub Security Advisory](https://github.com/CurvineIO/curvine/security/advisories)
  (with a CVE where appropriate), and the fix is noted in the
  [release notes](https://github.com/CurvineIO/curvine/releases).
- Reporters are credited in the advisory unless they ask not to be.

We recommend keeping your installation up to date with the latest release.

## Security Best Practices

When running Curvine, we recommend that you:

- Keep the software updated to the latest version
- Follow the principle of least privilege
- Review and audit your configurations regularly

Thank you for helping to keep Curvine and its users safe!
