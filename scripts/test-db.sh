#!/usr/bin/env bash
#
# Ensure a PostgreSQL instance is available for `npm run test:postgres`, then
# record its connection string in `.env` (gitignored) so the test scripts pick
# it up automatically via node's `--env-file-if-exists`.
#
# Prints the connection string on stdout; all logging goes to stderr, so this
# is safe to use as `POSTGRES_URL=$(scripts/test-db.sh)`.
#
# Backends, tried in order (override with KYSELY_HYDRATE_DB_BACKEND):
#   POSTGRES_URL  an instance you have already provisioned (CI does this)
#   docker        docker-compose.yml, mapped to port 5434
#   native        a Debian/Ubuntu packaged cluster driven by pg_ctlcluster,
#                 which is what Claude Code on the web provides
#
set -euo pipefail

readonly DB_NAME="kysely_hydrate_test"
readonly DB_USER="postgres"
readonly DB_PASSWORD="postgres"

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
readonly REPO_ROOT
readonly ENV_FILE="$REPO_ROOT/.env"

log() { printf '[test-db] %s\n' "$*" >&2; }
die() { printf '[test-db] error: %s\n' "$*" >&2; exit 1; }

build_url() {
	printf 'postgres://%s:%s@localhost:%s/%s' "$DB_USER" "$DB_PASSWORD" "$1" "$DB_NAME"
}

# Wait for an instance to accept connections. `pg_isready` comes with the
# Postgres client packages, which a machine that only ever talks to Postgres
# through node's `pg` need not have, so treat it as a best-effort check rather
# than failing a perfectly good instance for want of a CLI.
wait_until_ready() {
	local url="$1" attempts="${2:-30}" i
	if ! command -v pg_isready >/dev/null 2>&1; then
		log "pg_isready not installed; skipping readiness check"
		return 0
	fi
	for ((i = 1; i <= attempts; i++)); do
		if pg_isready -q -d "$url" 2>/dev/null; then
			return 0
		fi
		sleep 1
	done
	return 1
}

# Memoized: `docker info` talks to the daemon (or waits for a dead one to time
# out), and both the backend autodetection and `native_available` ask.
DOCKER_AVAILABLE=""
docker_available() {
	if [[ -z "$DOCKER_AVAILABLE" ]]; then
		if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
			DOCKER_AVAILABLE=yes
		else
			DOCKER_AVAILABLE=no
		fi
	fi
	[[ "$DOCKER_AVAILABLE" == "yes" ]]
}

# Driving a system-wide cluster sets the `postgres` password and creates a
# database, which we must not do to a developer's own machine behind their back.
# Running as root with no Docker available is our best proxy for "this is a
# throwaway container"; anyone who wants it elsewhere can ask for it explicitly
# with KYSELY_HYDRATE_DB_BACKEND=native.
native_available() {
	command -v pg_ctlcluster >/dev/null 2>&1 &&
		[[ "$(id -u)" == "0" ]] &&
		! docker_available
}

start_docker() {
	log "starting postgres via docker compose"
	docker compose --file "$REPO_ROOT/docker-compose.yml" up --detach --wait postgres >&2
	build_url 5434
}

start_native() {
	# Read the cluster's real coordinates rather than assuming 16/main:5432.
	local version cluster status port
	# `read` itself fails on empty input, and under `set -e` that would abort
	# with no diagnostic, so report the missing cluster here rather than after.
	read -r version cluster port status \
		< <(pg_lsclusters --no-header | awk 'NR == 1 { print $1, $2, $3, $4 }') ||
		die "no packaged postgres cluster found"

	if [[ "$status" != "online" ]]; then
		log "starting postgres cluster $version/$cluster on port $port"
		# Redirected because this function's stdout is the connection string.
		pg_ctlcluster "$version" "$cluster" start >&2
	else
		log "postgres cluster $version/$cluster already online on port $port"
	fi

	local url
	url="$(build_url "$port")"
	wait_until_ready "$url" || die "cluster $version/$cluster did not become ready"

	# Both statements are idempotent. Peer auth over the unix socket lets us in
	# as the cluster superuser without a password.
	log "ensuring role password and database $DB_NAME"
	su postgres -c "psql --quiet --no-psqlrc --command \"ALTER ROLE $DB_USER WITH PASSWORD '$DB_PASSWORD';\"" >&2
	su postgres -c "psql --quiet --no-psqlrc --tuples-only --no-align \
		--command \"SELECT 1 FROM pg_database WHERE datname = '$DB_NAME';\"" |
		grep --quiet 1 ||
		su postgres -c "createdb --owner $DB_USER $DB_NAME" >&2

	printf '%s' "$url"
}

# Replace any existing POSTGRES_URL line in place so repeated runs don't stack.
write_env_file() {
	local url="$1" tmp status
	# Alongside the target so the final `mv` is atomic: a reader never sees a
	# half-written .env, whatever happens in between.
	tmp="$(mktemp "$ENV_FILE.XXXXXX")"

	if [[ -f "$ENV_FILE" ]]; then
		status=0
		grep --invert-match '^POSTGRES_URL=' "$ENV_FILE" > "$tmp" || status=$?
		# Exit 1 just means every line matched; anything higher is a real error,
		# and carrying on would drop the user's other variables on the floor.
		((status <= 1)) || {
			rm -f -- "$tmp"
			die "could not read $ENV_FILE"
		}
	fi

	printf 'POSTGRES_URL=%s\n' "$url" >> "$tmp"
	mv "$tmp" "$ENV_FILE"
	log "wrote POSTGRES_URL to $ENV_FILE"
}

main() {
	local backend="${KYSELY_HYDRATE_DB_BACKEND:-auto}" url

	case "$backend" in
	auto)
		if [[ -n "${POSTGRES_URL:-}" ]]; then
			backend=preset
		elif docker_available; then
			backend=docker
		elif native_available; then
			backend=native
		else
			die "no postgres backend available. Start docker and re-run, or set POSTGRES_URL to an instance with a '$DB_NAME' database."
		fi
		;;
	esac

	case "$backend" in
	preset)
		url="${POSTGRES_URL:?POSTGRES_URL is not set}"
		log "using preset POSTGRES_URL"
		wait_until_ready "$url" 10 || die "cannot reach the postgres at POSTGRES_URL"
		;;
	docker) url="$(start_docker)" ;;
	native) url="$(start_native)" ;;
	*) die "unknown backend '$backend' (expected docker, native, or auto)" ;;
	esac

	write_env_file "$url"
	log "postgres ready"
	printf '%s\n' "$url"
}

main "$@"
