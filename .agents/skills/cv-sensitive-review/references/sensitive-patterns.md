# Sensitive data patterns

Catalogue behind `scripts/scan.sh` and the manual pass in `SKILL.md`.
Aligned with the classes used by GitHub secret scanning partner patterns, gitleaks,
trufflehog, and the compliance scopes Curvine is exposed to (PIPL / GDPR personal data,
customer business data, unpublished security findings).

Severity: **S0** live secret or production/customer data, **S1** internal infrastructure,
**S2** internal reference or PII-lite, **S3** noise candidate.

## 1. Cloud and object storage credentials (S0)

| Provider | Shape | Notes |
| -------- | ----- | ----- |
| AWS | `AKIA`/`ASIA`/`ABIA` + 16 upper-alnum, 40-char secret | secret alone is undetectable — pair detection needs both keys nearby |
| Alibaba Cloud | `LTAI` + ~20 alnum | OSS/ECS RAM key |
| Tencent Cloud | `AKID` + ~32 alnum | COS key |
| Huawei Cloud | AK/SK 20/40 char, no prefix | use entropy rule + `credentials`/`ak` context |
| Azure Storage | `AccountKey=<base64>` | connection-string form |
| GCP | service-account JSON (`"type": "service_account"`, `private_key`) | file-level block |
| S3-compatible / OCS / MinIO | opaque 32–40 char key with mixed case+digit, often with a `-` | entropy rule; **this is the class behind #2478** |
| Presigned URL | `X-Amz-Signature`, `OSSAccessKeyId`, `Signature=`, `Expires=` | grants object access without any key file |

Rules of thumb:

- A pair (endpoint + AK + SK) in a test/tool file is the most common Curvine-shaped leak: an
  internal UAT bucket committed "just for the integration test".
- Opaque random keys defeat name-based regex: pair the entropy rule with a context grep for
  `endpoint|credentials|access|secret|ak|sk|token`.

## 2. Keys, certificates, identity (S0)

- `-----BEGIN ... PRIVATE KEY-----` (RSA/EC/OPENSSH/PGP/DSA)
- `.pem`, `.key`, `.p12`, `.pfx`, `.jks`, `.keystore`, `id_rsa`, `id_ed25519`
- kubeconfig (`client-key-data`, `client-certificate-data`, `token:`, `users:`, server URLs)
- cloud CLI credential files (`.aws/credentials`, `.aliyun/config.json`, `.config/gcloud`)
- TLS material with private key, PFX export passwords

## 3. Tokens and SaaS credentials (S0)

| Class | Shape |
| ----- | ----- |
| GitHub PAT (classic) | `ghp_` / `gho_` / `ghu_` / `ghs_` / `ghr_` + 36 |
| GitHub PAT (fine-grained) | `github_pat_` + 40+ |
| GitLab PAT | `glpat-` + 20+ |
| Slack | `xoxb-`, `xoxp-`, `xoxa-`, `xoxr-`, `xoxs-` |
| JWT | three base64url segments starting `eyJ` |
| LLM providers | `sk-` + 20+ |
| npm / PyPI | `npm_` + 36, `pypi-AgEIcHlwaS5vcmc` |
| Webhooks | `https://hooks.slack.com/services/...`, Feishu/DingTalk `open-apis/bot/v2/hook/<uuid>` |
| CI secrets | `AKIA`, `DOCKER_PASSWORD`, `NPM_TOKEN`, `CARGO_REGISTRY_TOKEN` values in YAML |

A webhook URL is a write credential: treat it as S0, not as "just a URL".

## 4. Internal infrastructure (S1)

- RFC1918 literals: `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`, and `fc00::/7`
- Internal suffixes: `.internal`, `.intranet`, `.corp`, `.local`, `.lan`
- Organization-specific suffixes (UAT/prod object storage, CDN, SSO, artifact registries):
  supply via `CV_SENSITIVE_DOMAIN_SUFFIXES`, never hard-coded in the repo
- Anything that resolves internally: node/master/worker hostnames, `*-master-0`, `*-uat`, `*-prod`
- Database/queue URIs with inline credentials: `mysql://user:pass@`, `mongodb+srv://`, `redis://:pass@`, `amqp://`, `jdbc:...password=`
- Registry/mirror URLs with embedded basic-auth, internal Harbor/Nexus paths
- Cluster identifiers: kube context names, namespace + cluster name pairs, capacity/ACL descriptions

Why S1 and not S3: even without a credential, an internal endpoint is an entry point for
scanners and a confirmation of internal naming schemes. This is the exact shape that made
#2478 exploitable in combination with a key.

## 5. Internal references and PII-lite (S2)

- Internal ticket IDs: `[#1001227777]`, bare `22836620` — GitHub refs are short, internal
  trackers are 8–10 digits
- Corporate emails and handles (`@<corp>.com`), LDAP/AD accounts, employee names next to
  internal systems
- Internal wiki/dashboard/registry links, Grafana/Kibana/Rancher URLs
- Phone numbers (`1[3-9]xxxxxxxxx`), national ID (18-char with checksum), bank cards
- Public-but-unwanted data: personal device names, home-lab IPs, personal repo links

## 6. Customer, business and unreleased material (S0/S1)

Judgment only — no scanner:

- Customer or tenant names, their buckets/dirs, volumes, QPS, dataset names
- Revenue, pricing, contracts, capacity commitments, SLA targets
- Production incident details, exploited vulnerabilities, unfixed security findings
- Unreleased roadmap, benchmarks of unannounced hardware, unmerged design docs
- Screenshots/logs revealing any of the above, or another window's contents

## 7. Placeholders and false positives (S3)

Legitimate, do not block:

- `example.com`, `example.org`, `127.0.0.1`, `localhost`, `192.0.2.0/24`
- `test`, `dummy`, `fake`, `xxxx`, `<access-key>`, `${VAR}` / `$VAR` indirection
- lockfile checksums (`Cargo.lock`, `package-lock.json`, `go.sum`) — excluded by the scanner
- base64/hex test blobs, license headers, minified assets, `target/`

Dangerous near-miss: a "sanitized" example that keeps the real host with one character
changed, or a real key with a character removed. Redact the whole value.

## 8. Detection techniques

| Technique | Catches | Command |
| --------- | ------- | ------- |
| Provider regex | prefixed keys, URIs, tokens | `scan.sh` content mode |
| Context + entropy | opaque keys (OCS/MinIO/Huawei) | `scan.sh` entropy stage |
| Assignment regex | `password = "..."`, YAML/ENV blocks | `scan.sh` generic rule |
| Diff/PR range | what this change actually adds | `scan.sh --range origin/main...HEAD` |
| History search | secrets removed but still in history | `scan.sh --history '<literal>'`, `git log --all -S` |
| Manual read | topology, business data, screenshots | `SKILL.md` Step 3 |

Optional external tools when installed: `gitleaks detect --no-git -v`, `trufflehog filesystem`,
`detect-secrets scan`. The in-repo script is the floor, not the ceiling.
