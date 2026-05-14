#!/usr/bin/env bash
# Entrypoint: optional Curvine KV connector via env, then exec vLLM OpenAI server.
# All comments in English per project convention.

set -euo pipefail

if [[ "${VLLM_USE_CURVINE_KV:-0}" != "1" ]]; then
  exec vllm serve "$@"
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 required to build --kv-transfer-config JSON" >&2
  exit 1
fi

KV_JSON="$(python3 <<'PY'
import json
import os

backend = os.environ.get("CURVINE_KV_BACKEND", "posix").lower().strip()
extra: dict = {}

if backend == "native":
    extra["curvine_backend"] = "native"
    cfg_path = os.environ.get("CURVINE_SDK_CONFIG_PATH", "").strip()
    if not cfg_path:
        raise SystemExit(
            "CURVINE_SDK_CONFIG_PATH is required when CURVINE_KV_BACKEND=native"
        )
    extra["curvine_sdk_config_path"] = cfg_path
    extra["curvine_native_root"] = os.environ.get("CURVINE_NATIVE_ROOT", "/curvine_kv").strip()
elif backend == "posix":
    extra["curvine_backend"] = "posix"
    root = (
        os.environ.get("CURVINE_STORE_ROOT", "").strip()
        or os.environ.get("CURVINE_FUSE_MOUNT_ROOT", "").strip()
        or "/mnt/curvine"
    )
    extra["curvine_store_root"] = root
else:
    raise SystemExit(f"Unsupported CURVINE_KV_BACKEND={backend!r}; use posix or native")

if os.environ.get("CURVINE_MODEL_ID", "").strip():
    extra["curvine_model_id"] = os.environ["CURVINE_MODEL_ID"].strip()
if os.environ.get("CURVINE_TP_RANK", "").strip():
    extra["curvine_tp_rank"] = int(os.environ["CURVINE_TP_RANK"])
if os.environ.get("CURVINE_KV_GROUP_ID", "").strip():
    extra["curvine_kv_group_id"] = int(os.environ["CURVINE_KV_GROUP_ID"])
if os.environ.get("CURVINE_WRITE_CHUNK_NUM", "").strip():
    extra["curvine_write_chunk_num"] = int(os.environ["CURVINE_WRITE_CHUNK_NUM"])
if os.environ.get("CURVINE_WRITE_CHUNK_SIZE", "").strip():
    extra["curvine_write_chunk_size"] = int(os.environ["CURVINE_WRITE_CHUNK_SIZE"])

cfg = {
    "kv_connector": "CurvineKVConnector",
    "kv_role": "kv_both",
    "kv_connector_extra_config": extra,
}
print(json.dumps(cfg, separators=(",", ":")))
PY
)"

if [[ -n "${CURVINE_LD_PRELOAD:-}" ]]; then
  export LD_PRELOAD="${CURVINE_LD_PRELOAD}${LD_PRELOAD:+:$LD_PRELOAD}"
fi

exec vllm serve --kv-transfer-config "${KV_JSON}" "$@"
