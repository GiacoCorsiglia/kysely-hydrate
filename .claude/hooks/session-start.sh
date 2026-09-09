#!/usr/bin/env bash
#
# Prepare a Claude Code on the web container for `npm run test:all`.
#
# Node modules survive container snapshotting, but running services do not, so
# this has to start Postgres on every session even when the install is a no-op.
#
set -euo pipefail

# Local sessions manage their own environment.
if [[ "${CLAUDE_CODE_REMOTE:-}" != "true" ]]; then
	exit 0
fi

cd "${CLAUDE_PROJECT_DIR:-$(dirname -- "${BASH_SOURCE[0]}")/../..}"

# `install` rather than `ci` so a snapshotted node_modules is reused.
npm install --no-audit --no-fund

# Publish POSTGRES_URL to the session so ad-hoc psql and node commands inherit
# it. The npm test scripts read the .env file this writes, so they work anyway.
postgres_url="$(./scripts/test-db.sh)"
if [[ -n "${CLAUDE_ENV_FILE:-}" ]]; then
	# Quoted: a preset URL may carry characters the shell would otherwise eat.
	printf "export POSTGRES_URL='%s'\n" "$postgres_url" >> "$CLAUDE_ENV_FILE"
fi
