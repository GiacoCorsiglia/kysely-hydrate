#!/usr/bin/env bash
#
# Prepare a Claude Code on the web container for `npm run test:all`.
#
# node_modules survives container snapshotting but a running service does not,
# so Postgres has to be started on every session even when the install no-ops.
#
set -euo pipefail

# Local sessions manage their own environment.
[[ "${CLAUDE_CODE_REMOTE:-}" == "true" ]] || exit 0

cd "${CLAUDE_PROJECT_DIR:?CLAUDE_PROJECT_DIR is not set}"

# `ci` rather than `install`: the container ships a newer npm than the pinned
# packageManager, and `install` rewrites package-lock.json into every diff.
npm ci --no-audit --no-fund

# There is no Docker daemon here, so docker-compose.yml can't serve Postgres.
# Point the container's own cluster at the port the tests already expect
# instead of teaching them about a second one.
read -r version cluster < <(pg_lsclusters --no-header | awk 'NR == 1 { print $1, $2 }')
pg_conftool "$version" "$cluster" set port 5434
pg_ctlcluster "$version" "$cluster" restart
pg_isready --quiet --timeout 30 --port 5434

# Both are idempotent; peer auth over the unix socket gets us in as superuser.
su postgres -c "psql --quiet --port 5434 --command \
	\"ALTER ROLE postgres WITH PASSWORD 'postgres';\""
su postgres -c "psql --port 5434 --tuples-only --no-align --command \
	\"SELECT 1 FROM pg_database WHERE datname = 'kysely_hydrate_test';\"" | grep --quiet 1 ||
	su postgres -c "createdb --port 5434 kysely_hydrate_test"
