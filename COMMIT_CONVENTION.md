# Commit Message Convention

This project uses [Conventional Commits](https://www.conventionalcommits.org/) specification to standardize commit message format.

## ⚠️ Important Requirement

**All commit messages must include an associated Issue ID** in the format `(#number)`.

## Format

```
<type>[optional scope]: <description> (#issue-id)

[optional body]

[optional footer(s)]
```

## Types

- **feat**: New feature
- **fix**: Bug fix
- **docs**: Documentation updates
- **style**: Code style changes (no logic changes)
- **refactor**: Code refactoring
- **perf**: Performance optimization
- **test**: Test related
- **chore**: Build process or auxiliary tool changes
- **ci**: CI/CD related
- **build**: Build system or external dependency changes
- **revert**: Revert commit

## Examples

### ✅ Correct Commit Messages

```
feat: add user authentication system (#123)

Add JWT-based authentication with login and logout functionality.
```

```
fix: resolve memory leak in storage worker (#456)

Fixed issue where storage connections were not properly closed.
```

```
docs: update API documentation for v2.0 (#789)
```

```
refactor: optimize create_file, mkdir interfaces and reduce duplicate code (#53)
```

```
perf: improve database query performance (#234)

Optimized slow queries by adding proper indexes.

Closes #234
```

### ❌ Incorrect Commit Messages

```
✗ fix: resolve memory leak
   (missing issue ID)

✗ feat: Add new feature (#123)
   (subject starts with uppercase)

✗ fix: Fix the bug (#456).
   (ends with period)

✗ Fix bug (#789)
   (missing type)

✗ feat: add feature (123)
   (incorrect issue ID format, should be (#123))
```

## Rules

1. **Type must be one of the predefined types**
2. **Subject cannot be empty**
3. **Subject cannot end with period**
4. **Subject (excluding issue ID part) must start with lowercase letter**
5. **Must include issue ID in format `(#number)`**
6. **Header max length is 120 characters**
7. **Body and footer lines max length is 100 characters**

## Issue ID Requirements

- **Format**: Must be `(#number)`, e.g., `(#123)`
- **Position**: Must be at the end of commit message
- **Required**: All commits must include relevant issue ID
- **Association**: Issue ID should correspond to actual GitHub issue or PR

## Validation

Commit messages will be automatically validated through GitHub Actions during Pull Requests. If format doesn't comply with rules, CI will fail.

### Validation Rules

- ✅ `feat: add new feature (#123)` - Correct
- ❌ `feat: add new feature` - Missing issue ID
- ❌ `feat: add new feature #123` - Incorrect issue ID format
- ❌ `feat: add new feature (123)` - Missing # symbol
- ❌ `Feat: add new feature (#123)` - Type capitalized

### Local Validation

You can run the following commands locally to validate the latest commit:

```bash
npm install
npm run commitlint
```

## Recommended Workflow

1. **Create or find related Issue**: Every code change should correspond to a GitHub Issue
2. **Reference Issue in commit message**: Use `(#issue-number)` format
3. **Keep description concise and clear**: Explain what was done, not how it was done

## Additional Resources

- [Conventional Commits Specification](https://www.conventionalcommits.org/)
- [Angular Commit Format Reference](https://github.com/angular/angular/blob/main/CONTRIBUTING.md#commit)
- [GitHub Issue Linking Syntax](https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue) 