---
name: cv-address-pr-review
description: Review an existing Curvine PR, analyze review comments one by one with an accept/reject table, fix after user approval, update the PR, reply to comments, and resolve threads. Use when user asks to review a PR, handle review comments, or provides a PR URL.
---

# cv-address-pr-review

Process PR review comments systematically: analyze → propose → user approves → fix → update PR → reply → resolve.

## When to Use

- User asks to review / address comments on current branch PR
- User provides a PR URL or PR number
- User asks to read or respond to review feedback

## Step 1: Locate PR

```bash
# Current branch
gh pr view --json number,url,title,headRefName,baseRefName

# By number or URL
gh pr view <number> --json number,url,title,headRefName,baseRefName
```

## Step 2: Collect Comments

```bash
PR=<number>

# Inline review comments
gh api "repos/{owner}/{repo}/pulls/${PR}/comments?per_page=100"

# Conversation comments
gh api "repos/{owner}/{repo}/issues/${PR}/comments?per_page=100"

# Review summaries
gh pr view ${PR} --json reviews
```

Read **all** comments before proposing changes.

## Step 3: Analysis Table (Mandatory Before Coding)

Output a table for **every** comment ID. Do not post this table to GitHub.

```markdown
| Comment ID | Title / excerpt | Accept? | Reason |
| ---------- | --------------- | ------- | ------ |
| 1234567890 | "use Result instead of unwrap" | Yes | Valid error handling |
| 1234567891 | "rename to FooBar" | No | Conflicts with existing naming in module X |
```

Columns:

- **Comment ID** — numeric ID from API
- **Title / excerpt** — short summary of the comment
- **Accept?** — `Yes` / `No` / `Partial`
- **Reason** — technical justification

**Wait for user approval** on this table before making code changes.

## Step 4: Apply Fixes (Accepted Only)

For each accepted comment:

1. Minimal-scope code change
2. Run relevant tests (`make format` + targeted `cargo test`)
3. One focused commit per logical fix batch (user may prefer one commit — confirm)

## Step 5: Update PR

Follow [cv-create-pr](../cv-create-pr/SKILL.md) to push and refresh PR body if behavior or test results changed.

## Step 6: Reply to Each Comment (English)

### Inline review comment reply

```bash
gh api \
  -X POST \
  "repos/{owner}/{repo}/pulls/${PR}/comments/<COMMENT_ID>/replies" \
  -f body='Thanks. Addressed in <commit/ref>: <short description>.'
```

### Conversation comment

```bash
gh pr comment ${PR} --body "Thanks for the feedback. ..."
```

Reply templates:

- **Fixed:** `Thanks. Fixed in <sha>: <what changed>.`
- **Declined:** `Thanks for the suggestion. Kept current approach because <reason>.`
- **Already fixed:** `This is addressed in the latest push; no further change needed.`

## Step 7: Resolve Threads

For comments that are fixed, resolve the review thread via GraphQL:

```bash
# Get thread ID for a comment (GraphQL)
gh api graphql -f query='
query($owner:String!, $repo:String!, $pr:Int!) {
  repository(owner:$owner, name:$repo) {
    pullRequest(number:$pr) {
      reviewThreads(first:100) {
        nodes { id isResolved comments(first:1) { nodes { databaseId } } }
      }
    }
  }
}' -f owner=CurvineIO -f repo=curvine -F pr=${PR}
```

Resolve thread:

```bash
gh api graphql -f query='
mutation($threadId:ID!) {
  resolveReviewThread(input: {threadId:$threadId}) {
    thread { isResolved }
  }
}' -f threadId="<THREAD_ID>"
```

Resolve only threads where the fix is merged in the PR branch and reply is posted.

## Step 8: Checklist

- [ ] Every comment ID in analysis table
- [ ] User approved accept/reject decisions
- [ ] Fixes committed and pushed
- [ ] Each comment replied in English
- [ ] Fixed threads resolved on GitHub

## Related

- Create / update PR → [cv-create-pr](../cv-create-pr/SKILL.md)
