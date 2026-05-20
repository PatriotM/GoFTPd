#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  backup-user-state.sh [GOFTPD_ROOT] [BACKUP_DIR]

Backs up only GoFTPd user/auth runtime state, not site data:
  etc/passwd
  etc/passwd-
  etc/group
  etc/group-
  etc/users/
  etc/groups/
  etc/permissions.yml
  etc/affils.yml
  etc/slave_denylist.txt

Defaults:
  GOFTPD_ROOT = current directory
  BACKUP_DIR  = /var/backups/goftpd-users

Restore example:
  tar -tzf /var/backups/goftpd-users/goftpd-users-host-YYYYmmdd-HHMMSS.tar.gz
  tar -xzf /var/backups/goftpd-users/goftpd-users-host-YYYYmmdd-HHMMSS.tar.gz -C /GoFTPd_master
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

root="${1:-${GOFTPD_ROOT:-$(pwd)}}"
backup_dir="${2:-${GOFTPD_BACKUP_DIR:-/var/backups/goftpd-users}}"

if [[ ! -d "$root/etc" ]]; then
  echo "ERROR: $root does not look like a GoFTPd root; missing etc/." >&2
  exit 1
fi

mkdir -p "$backup_dir"

host="$(hostname -s 2>/dev/null || hostname 2>/dev/null || echo unknown)"
stamp="$(date +%Y%m%d-%H%M%S)"
archive="$backup_dir/goftpd-users-$host-$stamp.tar.gz"

paths=()
for item in \
  "etc/passwd" \
  "etc/passwd-" \
  "etc/group" \
  "etc/group-" \
  "etc/users" \
  "etc/groups" \
  "etc/permissions.yml" \
  "etc/affils.yml" \
  "etc/slave_denylist.txt"
do
  if [[ -e "$root/$item" ]]; then
    paths+=("$item")
  fi
done

if [[ ${#paths[@]} -eq 0 ]]; then
  echo "ERROR: no user/auth files found below $root." >&2
  exit 1
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

{
  echo "created_at=$(date -Is)"
  echo "host=$host"
  echo "root=$root"
  echo "files:"
  printf '  %s\n' "${paths[@]}"
} > "$tmpdir/MANIFEST.txt"

tar -czf "$archive" -C "$tmpdir" MANIFEST.txt -C "$root" "${paths[@]}"

chmod 0600 "$archive"

echo "Created user/auth backup:"
echo "$archive"
