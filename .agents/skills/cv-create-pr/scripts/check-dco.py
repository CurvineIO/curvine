#!/usr/bin/env python3
"""Check DCO sign-offs in the commits introduced by a PR head."""

import argparse
import re
import subprocess
import sys


# Match the trailer syntax checked by .github/workflows/dco.yml.
SIGN_OFF = re.compile(r"^Signed-off-by: (.+) <([^<>]+)>\s*$", re.IGNORECASE | re.MULTILINE)


def git(*args):
    return subprocess.check_output(["git", *args], encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base", help="Fetched commit of the actual PR base branch")
    parser.add_argument("head", nargs="?", default="HEAD", help="Proposed PR head")
    args = parser.parse_args()

    try:
        base, head = (
            git("rev-parse", "--verify", "--end-of-options", f"{ref}^{{commit}}").strip()
            for ref in (args.base, args.head)
        )
        records = git(
            "log",
            "-z",
            "--no-notes",
            "--no-show-signature",
            "--no-color",
            "--format=%H%x00%ae%x00%P%x00%B",
            f"{base}..{head}",
            "--",
        )
    except (subprocess.CalledProcessError, OSError, UnicodeError):
        print("DCO check could not read the proposed PR commit range.", file=sys.stderr)
        return 2

    fields = records.split("\0")[:-1]
    if len(fields) % 4 or (records and not records.endswith("\0")):
        print("DCO check received malformed Git log records.", file=sys.stderr)
        return 2
    if len(fields) // 4 > 250:
        print("GitHub DCO cannot verify over 250 PR commits; split the PR.", file=sys.stderr)
        return 1

    checked = merges = failures = 0
    for sha, author_email, parents, message in zip(
        fields[::4], fields[1::4], fields[2::4], fields[3::4]
    ):
        if len(parents.split()) > 1:
            merges += 1
            continue
        checked += 1
        emails = [email.lower() for _, email in SIGN_OFF.findall(message)]
        if author_email.lower() not in emails:
            reason = "sign-off does not match author email" if emails else "missing sign-off"
            print(f"{sha[:12]}: {reason}", file=sys.stderr)
            failures += 1

    if failures:
        return 1
    print(f"DCO PASS: {checked} non-merge commit(s) checked; {merges} merge commit(s) skipped.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
