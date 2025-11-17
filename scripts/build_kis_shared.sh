#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PACKAGE_DIR="$REPO_ROOT/packages/kis_api"
OUT_DIR="$REPO_ROOT/dist"

if python -m build --wheel --outdir "$OUT_DIR" "$PACKAGE_DIR"; then
  echo "Built kis_api wheel under $OUT_DIR via python -m build"
  exit 0
fi

echo "python -m build 실패, pip wheel로 대체 시도 중..."
python -m pip wheel "$PACKAGE_DIR" -w "$OUT_DIR"
echo "Built kis_api wheel under $OUT_DIR via pip wheel"
