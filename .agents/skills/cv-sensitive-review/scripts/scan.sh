#!/usr/bin/env bash
# cv-sensitive-review scanner: pre-submit sensitive data gate for Curvine.
#
# Reports findings as:
#   [S1] internal-private-ip  path/to/file.rs:42
# The matched text is NEVER printed, so the scanner is safe to run in CI logs and
# safe to paste into an issue. Use --show-match only on your own terminal.
#
# Exit codes: 0 PASS, 1 BLOCK (S0/S1), 3 REVIEW (S2 only), 2 usage error.
set -uo pipefail

usage() {
  cat <<'USAGE'
Usage: scan.sh [MODE] [TARGET...]

Modes (default: content, targets are files or '-' for stdin):
  (files...)            scan the given files
  --staged              scan files staged for commit
  --range <A..B>        scan files added/modified in a git range (content at B)
  --repo                scan every tracked file (repository audit)
  --history <literal>   find commits that ever added/removed the literal value

Options:
  --show-match          print the matched text (local terminal only, never in CI)
  --quiet               only print the summary line
  -h, --help            this help

Environment:
  CV_SENSITIVE_DOMAIN_SUFFIXES   comma list of internal domain suffixes
                                 (default: internal,intranet,corp,local,lan)
  CV_INTERNAL_EMAIL_DOMAINS      comma list of corporate email domains
  CV_SKIP_PATHS                  extra skip regex for paths

Exit codes: 0 PASS, 1 BLOCK (S0/S1), 3 REVIEW (S2 only), 2 usage error.
USAGE
}

MODE="content"
RANGE=""
HISTORY_LITERAL=""
SHOW_MATCH=0
QUIET=0
TARGETS=()

DOMAIN_SUFFIXES="${CV_SENSITIVE_DOMAIN_SUFFIXES:-}"
EMAIL_DOMAINS="${CV_INTERNAL_EMAIL_DOMAINS:-}"
SKIP_EXTRA="${CV_SKIP_PATHS:-}"

SKIP_RE="(^|/)\.git/|(^|/)target/|(^|/)node_modules/|(^|/)vendor/|\.agents/skills/cv-sensitive-review/|(^|/)Cargo\.lock$|(^|/)package-lock\.json$|\.min\.(js|css)$|\.map$"
if [[ -n "$SKIP_EXTRA" ]]; then
  SKIP_RE="$SKIP_RE|$SKIP_EXTRA"
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --staged) MODE="staged"; shift ;;
    --range) MODE="range"; RANGE="${2:-}"; shift 2 ;;
    --repo) MODE="repo"; shift ;;
    --history) MODE="history"; HISTORY_LITERAL="${2:-}"; shift 2 ;;
    --show-match) SHOW_MATCH=1; shift ;;
    --quiet) QUIET=1; shift ;;
    -h|--help) usage; exit 0 ;;
    --) shift; while [[ $# -gt 0 ]]; do TARGETS+=("$1"); shift; done ;;
    -) TARGETS+=("-"); shift ;;
    -*) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    *) TARGETS+=("$1"); shift ;;
  esac
done

# ---------------------------------------------------------------------------
# Rule table: severity|label|extended-regex  (# starts a comment)
# Severity: S0 live secret / prod data, S1 internal infra, S2 identifiers / PII-lite.
# ---------------------------------------------------------------------------
IFS= read -r -d '' RULES <<'RULES' || true
S0|private-key-block|-----BEGIN ([A-Z0-9]+ )?PRIVATE KEY-----
S0|aws-access-key|(AKIA|ASIA|ABIA|ACCA)[0-9A-Z]{16}
S0|aliyun-access-key|LTAI[0-9A-Za-z]{12,24}
S0|tencent-secret-id|AKID[0-9A-Za-z]{13,40}
S0|github-token|gh[pousr]_[0-9A-Za-z]{36,}
S0|github-fine-grained-token|github_pat_[0-9A-Za-z_]{40,}
S0|gitlab-token|glpat-[0-9A-Za-z_-]{20,}
S0|slack-token|xox[baprs]-[0-9A-Za-z-]{10,}
S0|jwt-token|eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}
S0|llm-api-key|sk-[A-Za-z0-9]{20,}
S0|azure-account-key|AccountKey=[A-Za-z0-9+/=]{40,}
S0|gcp-service-account|"type"[[:space:]]*:[[:space:]]*"service_account"
S0|presigned-url|(X-Amz-Signature|X-Amz-Credential|OSSAccessKeyId|AWSAccessKeyId|Signature)=[A-Za-z0-9%+/=_-]{8,}
S0|uri-with-password|(mysql|postgres|postgresql|mongodb|mongodb\+srv|redis|rediss|amqp|amqps|clickhouse|kafka)://[^:@/[:space:]]+:[^@/[:space:]]{3,}@
S0|jdbc-with-password|jdbc:[a-z0-9]+://[^[:space:]]*[pP]assword=[^[:space:]&"']{3,}
S0|kubeconfig-secret|(client-key-data|client-certificate-data)[[:space:]]*:[[:space:]]*[A-Za-z0-9+/=]{40,}
S0|webhook-url|https?://(hooks\.slack\.com/services|open\.apis\.(feishu|dingtalk)[^[:space:]]*/hook/)
S1|generic-credential-assignment|([Aa]ccess[_-]?[Kk]ey|[Ss]ecret[_-]?([Kk]ey|[Ii]d)|[Aa]pi[_-]?[Kk]ey|[Pp]assw(or)?d|[Tt]oken)["']?[[:space:]]*[:=][[:space:]]*["'][^"'{}$[:space:]]{8,}["']
S1|internal-private-ip|(^|[^0-9.])(10\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}|192\.168\.[0-9]{1,3}\.[0-9]{1,3}|172\.(1[6-9]|2[0-9]|3[01])\.[0-9]{1,3}\.[0-9]{1,3})([^0-9.]|$)
S1|internal-domain-suffix|(^|[^A-Za-z0-9_.-])[A-Za-z0-9][A-Za-z0-9.-]*\.(internal|intranet|corp|local|lan)([^A-Za-z0-9_.-]|$)
S1|cn-national-id|[^0-9]([1-9][0-9]{5}(19|20)[0-9]{2}(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])[0-9]{3}[0-9Xx])([^0-9]|$)
S2|internal-ticket-bracket|\[#[0-9]{6,}\]
S2|internal-ticket-bare|(^|[^0-9A-Za-z])#[0-9]{8,10}([^0-9]|$)
S2|cn-mobile-number|(^|[^0-9])1[3-9][0-9]{9}([^0-9]|$)
S2|bank-card-number|(^|[^0-9])(4[0-9]{12}([0-9]{3})?|5[1-5][0-9]{14}|62[0-9]{14,17})([^0-9]|$)
RULES

# Organization-specific suffixes / email domains are appended at runtime so the
# repository never hard-codes its own internal naming scheme.
if [[ -n "$DOMAIN_SUFFIXES" ]]; then
  suffix_alt="$(printf '%s' "$DOMAIN_SUFFIXES" | tr ',' '|' | tr -d ' ')"
  RULES+=$ '\n'"S1|org-internal-domain|(^|[^A-Za-z0-9_.-])[A-Za-z0-9][A-Za-z0-9.-]*\.($suffix_alt)([^A-Za-z0-9_.-]|$)"
fi
if [[ -n "$EMAIL_DOMAINS" ]]; then
  email_alt="$(printf '%s' "$EMAIL_DOMAINS" | tr ',' '|' | tr -d ' ')"
  RULES+=$'\n'"S2|corporate-email|[A-Za-z0-9._%+-]+@($email_alt)([^A-Za-z0-9]|$)"
fi

BLOCK=0
REVIEW=0
FINDINGS=0

# Documented-safe values that appear in real code and docs. Keep this list narrow:
# anything added here stops being reported forever.
ALLOW_RE='AKIAIOSFODNN7EXAMPLE|wJalrXUtnFEMI/K7MDENG[+/]bPxRfiCYEXAMPLEKEY|host\.docker\.internal|([^0-9]|^)127\.0\.0\.1([^0-9]|$)|([^0-9A-Za-z]|^)localhost([^0-9A-Za-z]|$)|example\.(com|org|net)|\[::1\]|\.svc\.cluster\.local|cluster\.local|env\.local|\$\.example|db\.example|postgres-host|<your|user:password|s3\.credentials|<host>|<node|<ip>|\$[{][A-Za-z_]+[}]|xxxx|dummy|placeholder|0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'

is_allowed() {
  grep -Eq "$ALLOW_RE" <<< "$1"
}

report() {
  local sev="$1" label="$2" loc="$3" line_text="${4:-}"
  FINDINGS=$((FINDINGS + 1))
  case "$sev" in
    S0|S1) BLOCK=$((BLOCK + 1)) ;;
    *) REVIEW=$((REVIEW + 1)) ;;
  esac
  if [[ $QUIET -eq 0 ]]; then
    printf '[%s] %-32s %s\n' "$sev" "$label" "$loc"
    if [[ $SHOW_MATCH -eq 1 && -n "$line_text" ]]; then
      printf '      > %s\n' "$line_text"
    fi
  fi
}

skipped() {
  printf '%s' "$1" | grep -Eq "$SKIP_RE"
}

scan_file() {
  local path="$1" display="$2" rule sev label re out ln text
  [[ -f "$path" ]] || return 0
  skipped "$display" && return 0
  while IFS='|' read -r sev label re; do
    [[ -z "$sev" || "$sev" == \#* ]] && continue
    while IFS= read -r out; do
      [[ -z "$out" ]] && continue
      ln="${out%%:*}"
      text="${out#*:}"
      is_allowed "$text" && continue
      lsev="$sev"
      case "$label" in
        internal-private-ip)
          # Bare RFC1918 in prose is a hint; the same IP with a port, inside a URL or
          # in a docker push/pull is a reachable internal endpoint.
          if ! grep -Eq ':[0-9]{2,5}([^0-9]|$)|https?://|docker (push|pull|tag)' <<< "$text"; then
            lsev=S2
          fi
          ;;
        generic-credential-assignment)
          # A dotted lowercase value is a config key path (s3.credentials.access),
          # not a secret literal.
          if grep -Eq "=[[:space:]]*[\"'][a-z][a-z0-9_]*(\.[a-z0-9_]+)+[\"']" <<< "$text"; then
            continue
          fi
          ;;
      esac
      report "$lsev" "$label" "$display:$ln" "$text"
    done < <(grep -E -n -I --binary-files=without-match -e "$re" -- "$path" 2>/dev/null)
  done <<< "$RULES"

  # Entropy stage: opaque credentials that no name-based rule can catch
  # (object storage / OCS style random keys, Huawei AK/SK, generic API secrets).
  command -v python3 >/dev/null 2>&1 || return 0
  local ctx_re='[Cc]redential|[Aa]ccess|[Ss]ecret|[Tt]oken|[Pp]assword|[Aa]uth|[Kk]ey|\bAK\b|\bSK\b'
  while IFS= read -r out; do
    [[ -z "$out" ]] && continue
    local ctx_text
    ctx_text="$(sed -n "${out}p" "$path" 2>/dev/null)"
    is_allowed "$ctx_text" && continue
    # A high-entropy string sitting next to a credential keyword is a credential,
    # not a hint: escalate it to blocking.
    # Credential keywords usually sit on the line above (config key / field name),
    # so widen the context window by one line on each side.
    if sed -n "$((out > 1 ? out - 1 : 1)),$((out + 1))p" "$path" 2>/dev/null | grep -Eq "$ctx_re"; then
      report "S1" "high-entropy-credential" "$display:$out" ""
    else
      report "S2" "high-entropy-string" "$display:$out" ""
    fi
  done < <(python3 - "$path" <<'PY' 2>/dev/null
import math
import re
import sys

PATH = sys.argv[1]
CANDIDATE = re.compile(r"""["']([A-Za-z0-9+/=_-]{32,})["']""")
HASH_CONTEXT = re.compile(r"(sha256|sha1|sha512|md5|integrity|digest|checksum|revision|commit|uuid)", re.I)
ALL_HEX = re.compile(r"^[0-9a-fA-F]+$")


def entropy(s):
    if not s:
        return 0.0
    counts = {}
    for ch in s:
        counts[ch] = counts.get(ch, 0) + 1
    total = len(s)
    return -sum((c / total) * math.log2(c / total) for c in counts.values())


try:
    fh = open(PATH, "r", errors="ignore")
except OSError:
    sys.exit(0)

with fh:
    for lineno, line in enumerate(fh, 1):
        if len(line) > 4000 or HASH_CONTEXT.search(line):
            continue
        for m in CANDIDATE.finditer(line):
            s = m.group(1)
            if ALL_HEX.match(s) and len(s) in (32, 40, 64, 128):
                continue
            if not (re.search(r"[0-9]", s) and re.search(r"[A-Z]", s) and re.search(r"[a-z]", s)):
                continue
            if entropy(s) >= 4.0:
                print(lineno)
                break
PY
  )
}

scan_history() {
  local literal="$1" hits
  hits="$(git log --all --format='%h %ad %s' --date=short -S"$literal" 2>/dev/null)"
  if [[ -n "$hits" ]]; then
    report "S0" "literal-in-git-history" "(literal, $(printf '%s\n' "$hits" | wc -l | tr -d ' ') commits)"
    [[ $QUIET -eq 0 ]] && printf '%s\n' "$hits" | sed 's/^/      /'
  fi
}

staged_files() {
  git diff --cached --name-only --diff-filter=AM 2>/dev/null
}

range_files() {
  local rev="${1##*..}"
  git diff --name-only --diff-filter=AM "$1" 2>/dev/null | while IFS= read -r p; do
    printf '%s\t%s\n' "$rev" "$p"
  done
}

case "$MODE" in
  history)
    [[ -z "$HISTORY_LITERAL" ]] && { echo "--history needs a literal" >&2; exit 2; }
    scan_history "$HISTORY_LITERAL"
    ;;
  staged)
    while IFS= read -r f; do
      [[ -z "$f" ]] && continue
      scan_file "$f" "$f"
    done < <(staged_files)
    ;;
  range)
    [[ -z "$RANGE" ]] && { echo "--range needs a range like origin/main...HEAD" >&2; exit 2; }
    if [[ "$RANGE" != *..* ]]; then
      echo "--range expects <base>..<head>" >&2
      exit 2
    fi
    tmpdir="$(mktemp -d)"
    trap 'rm -rf "$tmpdir"' EXIT
    while IFS=$'\t' read -r rev f; do
      [[ -z "$f" ]] && continue
      skipped "$f" && continue
      mkdir -p "$tmpdir/$(dirname "$f")"
      git show "$rev:$f" >"$tmpdir/$f" 2>/dev/null || continue
      scan_file "$tmpdir/$f" "$f"
    done < <(range_files "$RANGE")
    ;;
  repo)
    while IFS= read -r f; do
      [[ -z "$f" ]] && continue
      scan_file "$f" "$f"
    done < <(git ls-files 2>/dev/null)
    ;;
  content)
    if [[ ${#TARGETS[@]} -eq 0 ]]; then
      echo "nothing to scan" >&2
      usage >&2
      exit 2
    fi
    for t in "${TARGETS[@]}"; do
      if [[ "$t" == "-" ]]; then
        tmp="$(mktemp)"
        cat >"$tmp"
        scan_file "$tmp" "stdin"
        rm -f "$tmp"
      elif [[ -d "$t" ]]; then
        while IFS= read -r f; do
          scan_file "$f" "${f#"$t"/}"
        done < <(find "$t" -type f -not -path '*/.git/*' 2>/dev/null)
      else
        scan_file "$t" "$t"
      fi
    done
    ;;
esac

if [[ $FINDINGS -eq 0 ]]; then
  echo "PASS: no sensitive data found"
  exit 0
fi

if [[ $BLOCK -gt 0 ]]; then
  echo "BLOCK: $BLOCK blocking finding(s) (S0/S1), $REVIEW review finding(s) (S2)"
  echo "See .agents/skills/cv-sensitive-review/SKILL.md Step 4 for the required action."
  exit 1
fi

echo "REVIEW: $REVIEW finding(s) (S2) - redact or record an explicit decision before publishing"
exit 3
