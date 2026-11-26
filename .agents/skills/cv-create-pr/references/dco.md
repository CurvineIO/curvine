# DCO checks and recovery

Curvine's [DCO workflow](../../../../.github/workflows/dco.yml) requires a
`Signed-off-by: Name <email>` line with an email matching the commit author's
email (case-insensitive). Sign-off is a certification under the project
[DCO](../../../../DCO), not a cryptographic signature. Multiple sign-offs on
one commit do not compensate for a missing sign-off on another.

## Check the proposed PR range

1. For an existing PR, use `gh pr view <PR> --json baseRefName,baseRefOid,headRefName,headRefOid,headRepositoryOwner,isCrossRepository`.
   Fetch the actual base and head and verify the SHAs; identify the remote
   that owns the head branch, including for a fork. For a new PR, fetch its
   intended target branch. Use the target commit, not a hard-coded `main`.
2. From the repository root, run:

   ```bash
   python3 .agents/skills/cv-create-pr/scripts/check-dco.py "$BASE_SHA" HEAD
   ```

   This checks `BASE_SHA..HEAD` and reports missing or mismatched sign-offs by
   SHA without printing email addresses. Like CI, it skips merge commits.
   Unlike CI, it cannot tell which GitHub accounts are bots; check any apparent
   bot exception against the GitHub commit API. GitHub remains authoritative.
3. For a failed PR check, confirm that the job belongs to the current PR head
   before interpreting it. Use `gh pr checks <PR> --json name,state,link`,
   check the job's `head_sha` with
   `gh api repos/<owner>/<repo>/actions/jobs/<JOB_ID> --jq .head_sha`, and read
   the failure with `gh run view --job <JOB_ID> --log-failed`. A commit-count
   mismatch is not a missing trailer: compare the live PR's commit count and
   list, especially after a retarget or concurrent push. An old run may contain
   stale PR metadata; check a new run triggered by a fresh PR event rather than
   rerunning the old event. Split a PR exceeding GitHub's 250-commit API limit.

## Repair missing or mismatched sign-offs

- Work from the exact published head in a clean, preferably separate worktree.
  Save its SHA as `OLD_HEAD` and keep a local reference to it for comparison.
  Examine authors with `git show -s --format=fuller <SHA>` and confirm that
  the committer identity used for sign-off matches the author and that the
  author has authorized the certification. Do not change authorship or add
  someone else's sign-off to make the check pass. Ask the commit author to
  sign when their sign-off is missing and you cannot certify as that author.
- If only the tip needs repair, run `git commit --amend --no-edit --signoff`.
  For earlier commits in a linear branch, run `git rebase -i <parent-of-first-affected-commit>`
  and mark only the commits needing your authorized sign-off as `edit`. At each
  stop, amend as above and run `git rebase --continue`. Avoid blanket sign-off
  across mixed authors and avoid flattening merges or changing the PR base.
- Compare the old and new ranges with `git range-diff "$BASE_SHA..$OLD_HEAD" "$BASE_SHA..HEAD"`
  and require `git diff --exit-code "$OLD_HEAD" HEAD` to pass for a trailer-only
  repair. Rerun the local DCO check and the sensitive-data gate on the proposed
  messages and PR diff.
- Coordinate the history rewrite with branch users. Recheck the live base and
  head immediately before pushing. Use an **explicit lease** against the
  original head SHA on the PR's head remote, never a plain `--force`:

  ```bash
  HEAD_REF="refs/heads/$HEAD_BRANCH"
  git push --force-with-lease="$HEAD_REF:$OLD_HEAD" "$HEAD_REMOTE" "HEAD:$HEAD_REF"
  ```

  If the lease rejects the update, stop and inspect the new remote head. Refresh
  downstream stacked branches separately if the rewrite affects them.
- Verify that GitHub shows the new head SHA and that its `dco` check passes.
  A green check on the previous head is not evidence that this repair passed.
