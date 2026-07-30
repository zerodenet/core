#!/usr/bin/env bash
#
# Manage the Zero version contract and publish a sealed release.
#
# Usage:
#   ./scripts/release.sh --check
#   ./scripts/release.sh 0.0.16 --dry-run
#   ./scripts/release.sh 0.0.16 --no-push
#   ./scripts/release.sh 0.0.17-dev --start-development
set -euo pipefail

MODE=release
DRY_RUN=false
NO_PUSH=false
SEAL_ONLY=false
MESSAGE=""
VERSION=""

usage() {
    sed -n '2,10p' "$0"
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --check) MODE=check; shift ;;
        --check-release) MODE=check-release; shift ;;
        --start-development) MODE=start-development; shift ;;
        --dry-run) DRY_RUN=true; shift ;;
        --no-push) NO_PUSH=true; shift ;;
        --seal-only) SEAL_ONLY=true; shift ;;
        -m) MESSAGE="$2"; shift 2 ;;
        --help|-h) usage ;;
        -*) echo "Unknown option: $1"; usage ;;
        *)
            if [[ -n "$VERSION" ]]; then
                echo "Version already set to '$VERSION', unexpected argument: $1"
                usage
            fi
            VERSION="$1"
            shift
            ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "${ZERO_REPO_ROOT:-$SCRIPT_DIR/..}"

CARGO_TOML=Cargo.toml
BREAKING_CHANGES=release/breaking-changes.md
ROW_MARKER='<!-- version-contract:unreleased-row -->'
EMPTY_ROW="| \`Unreleased\` | - | No pending compatibility changes ${ROW_MARKER} |"
EMPTY_BODY_COMMENT='<!-- Record implemented but unsealed compatibility changes here. -->'

fail() {
    echo "Version contract error: $*" >&2
    exit 1
}

require_files() {
    [[ -f "$CARGO_TOML" && -f "$BREAKING_CHANGES" ]] || \
        fail "Cargo.toml or breaking-changes.md was not found in $(pwd)."
}

validate_version() {
    local version=$1 expected=$2
    [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z][0-9A-Za-z.-]*)?$ ]] || \
        fail "invalid version '$version'; expected X.Y.Z or X.Y.Z-suffix"
    if [[ "$expected" == development && "$version" != *-dev ]]; then
        fail "development version must end with '-dev'"
    fi
    if [[ "$expected" == release && "$version" == *-dev ]]; then
        fail "release version must not end with '-dev'"
    fi
}

workspace_version() {
    local path=${1:-$CARGO_TOML}
    awk '
        /^\[workspace\.package\][[:space:]]*$/ { in_workspace=1; next }
        in_workspace && /^\[/ { exit }
        in_workspace && /^version[[:space:]]*=/ {
            if (match($0, /"[^"]+"/)) {
                print substr($0, RSTART + 1, RLENGTH - 2)
                exit
            }
        }
    ' "$path"
}

unreleased_row() {
    local path=${1:-$BREAKING_CHANGES}
    grep -F "$ROW_MARKER" "$path" | tr -d '\r'
}

unreleased_body() {
    local path=${1:-$BREAKING_CHANGES}
    awk '
        { sub(/\r$/, "") }
        /^## Unreleased$/ { inside=1; next }
        inside && /^## / { exit }
        inside { print }
    ' "$path"
}

body_is_substantive() {
    awk '
        { sub(/\r$/, "") }
        /^[[:space:]]*$/ { next }
        /^[[:space:]]*<!--.*-->[[:space:]]*$/ { next }
        { found=1 }
        END { exit(found ? 0 : 1) }
    '
}

assert_common_contract() {
    local breaking=${1:-$BREAKING_CHANGES}
    local marker_count heading_count
    marker_count=$(grep -Fc "$ROW_MARKER" "$breaking" || true)
    heading_count=$(grep -Ec '^## Unreleased\r?$' "$breaking" || true)
    [[ "$marker_count" == 1 ]] || fail "compatibility matrix must contain one marked Unreleased row"
    [[ "$heading_count" == 1 ]] || fail "breaking changes must contain one '## Unreleased' section"
    [[ "$(unreleased_row "$breaking")" == \|\ \`Unreleased\`\ \|* ]] || \
        fail "unreleased row marker must be on the Unreleased matrix row"
}

assert_development_contract() {
    local cargo=${1:-$CARGO_TOML} breaking=${2:-$BREAKING_CHANGES}
    local current
    current=$(workspace_version "$cargo")
    [[ -n "$current" ]] || fail "workspace package version was not found"
    validate_version "$current" development
    assert_common_contract "$breaking"
    if grep -Fq "$current" "$breaking"; then
        fail "development version '$current' must not be bound into the compatibility ledger"
    fi
    echo "$current"
}

assert_unsealed_contract() {
    local cargo=${1:-$CARGO_TOML} breaking=${2:-$BREAKING_CHANGES}
    local current
    current=$(workspace_version "$cargo")
    [[ -n "$current" ]] || fail "workspace package version was not found"
    if [[ "$current" == *-dev ]]; then
        validate_version "$current" development
        if grep -Fq "$current" "$breaking"; then
            fail "development version '$current' must not be bound into the compatibility ledger"
        fi
    else
        validate_version "$current" release
    fi
    assert_common_contract "$breaking"
    echo "$current"
}

assert_release_contract() {
    local cargo=$1 breaking=$2 release_version=$3
    local current row
    validate_version "$release_version" release
    current=$(workspace_version "$cargo")
    [[ "$current" == "$release_version" ]] || \
        fail "Cargo version '$current' does not match release '$release_version'"
    assert_common_contract "$breaking"
    row=$(unreleased_row "$breaking")
    [[ "$row" == "$EMPTY_ROW" ]] || fail "release requires an empty Unreleased matrix row"
    if unreleased_body "$breaking" | body_is_substantive; then
        fail "release requires an empty Unreleased section"
    fi
    grep -Eq "^## ${release_version//./\\.}\r?$" "$breaking" || \
        fail "breaking changes has no release section for '$release_version'"
    grep -Fq "| \`${release_version}\` |" "$breaking" || \
        fail "compatibility matrix has no release row for '$release_version'"
}

render_cargo_version() {
    local source=$1 destination=$2 version=$3
    awk -v version="$version" '
        { sub(/\r$/, "") }
        /^\[workspace\.package\][[:space:]]*$/ { in_workspace=1 }
        in_workspace && /^version[[:space:]]*=/ && !changed {
            sub(/"[^"]+"/, "\"" version "\"")
            changed=1
        }
        { print }
        END { if (!changed) exit 2 }
    ' "$source" > "$destination"
}

render_release_docs() {
    local source=$1 destination=$2 version=$3 has_changes=$4
    awk \
        -v version="$version" \
        -v marker="$ROW_MARKER" \
        -v empty_row="$EMPTY_ROW" \
        -v empty_comment="$EMPTY_BODY_COMMENT" \
        -v has_changes="$has_changes" '
        { sub(/\r$/, "") }
        index($0, marker) {
            released=$0
            sub(/`Unreleased`/, "`" version "`", released)
            gsub(" " marker, "", released)
            sub(/[[:space:]]+\|$/, " |", released)
            print empty_row
            print released
            row_changed=1
            next
        }
        /^## Unreleased$/ {
            print "## Unreleased"
            print ""
            print empty_comment
            print ""
            print "## " version
            if (has_changes == "false") {
                print ""
                print "<!-- No compatibility changes in this release. -->"
                print ""
                skip_released_body=1
            }
            heading_changed=1
            next
        }
        skip_released_body && /^## / { skip_released_body=0 }
        skip_released_body { next }
        { print }
        END { if (!row_changed || !heading_changed) exit 2 }
    ' "$source" > "$destination"
}

prepare_release_contract() {
    local release_version=$1 dry_run=$2
    local current cargo_tmp docs_tmp has_changes
    current=$(assert_unsealed_contract "$CARGO_TOML" "$BREAKING_CHANGES")
    validate_version "$release_version" release
    if grep -Eq "^## ${release_version//./\\.}\r?$" "$BREAKING_CHANGES"; then
        fail "release '$release_version' already exists in breaking changes"
    fi
    has_changes=false
    if unreleased_body "$BREAKING_CHANGES" | body_is_substantive; then
        has_changes=true
    fi
    cargo_tmp=$(mktemp "${CARGO_TOML}.version.XXXXXX")
    docs_tmp=$(mktemp "${BREAKING_CHANGES}.version.XXXXXX")
    trap 'rm -f "${cargo_tmp:-}" "${docs_tmp:-}"' RETURN
    render_cargo_version "$CARGO_TOML" "$cargo_tmp" "$release_version"
    render_release_docs "$BREAKING_CHANGES" "$docs_tmp" "$release_version" "$has_changes"
    assert_release_contract "$cargo_tmp" "$docs_tmp" "$release_version"

    if [[ "$dry_run" == true ]]; then
        git diff --no-index --ignore-space-at-eol -- "$CARGO_TOML" "$cargo_tmp" || true
        git diff --no-index --ignore-space-at-eol -- "$BREAKING_CHANGES" "$docs_tmp" || true
    else
        mv "$cargo_tmp" "$CARGO_TOML"
        mv "$docs_tmp" "$BREAKING_CHANGES"
        trap - RETURN
    fi
    echo "Release contract prepared: $current -> $release_version"
}

start_development() {
    local version=$1 dry_run=$2 current cargo_tmp
    validate_version "$version" development
    current=$(workspace_version "$CARGO_TOML")
    if [[ "$current" == *-dev ]]; then
        assert_development_contract "$CARGO_TOML" "$BREAKING_CHANGES" >/dev/null
    elif [[ "$(unreleased_row "$BREAKING_CHANGES")" != "$EMPTY_ROW" ]] || \
        unreleased_body "$BREAKING_CHANGES" | body_is_substantive; then
        assert_unsealed_contract "$CARGO_TOML" "$BREAKING_CHANGES" >/dev/null
    else
        assert_release_contract "$CARGO_TOML" "$BREAKING_CHANGES" "$current"
    fi
    cargo_tmp=$(mktemp "${CARGO_TOML}.version.XXXXXX")
    trap 'rm -f "${cargo_tmp:-}"' RETURN
    render_cargo_version "$CARGO_TOML" "$cargo_tmp" "$version"
    assert_development_contract "$cargo_tmp" "$BREAKING_CHANGES" >/dev/null
    if [[ "$dry_run" == true ]]; then
        diff -u "$CARGO_TOML" "$cargo_tmp" || true
    else
        mv "$cargo_tmp" "$CARGO_TOML"
        trap - RETURN
    fi
    echo "Development contract prepared: $current -> $version"
}

assert_clean_tree() {
    [[ -z "$(git status --porcelain)" ]] || \
        fail "working tree is not clean; commit or stash changes before changing versions"
}

require_files

case "$MODE" in
    check)
        current=$(workspace_version "$CARGO_TOML")
        if [[ "$current" == *-dev ]]; then
            assert_development_contract "$CARGO_TOML" "$BREAKING_CHANGES" >/dev/null
            echo "Development contract is valid ($current, Unreleased)."
        elif [[ "$(unreleased_row "$BREAKING_CHANGES")" != "$EMPTY_ROW" ]] || \
            unreleased_body "$BREAKING_CHANGES" | body_is_substantive; then
            assert_unsealed_contract "$CARGO_TOML" "$BREAKING_CHANGES" >/dev/null
            echo "Unsealed release contract is valid ($current, Unreleased)."
        else
            assert_release_contract "$CARGO_TOML" "$BREAKING_CHANGES" "$current"
            echo "Release contract is valid ($current)."
        fi
        exit 0
        ;;
    check-release)
        [[ -n "$VERSION" ]] || fail "--check-release requires a version"
        assert_release_contract "$CARGO_TOML" "$BREAKING_CHANGES" "$VERSION"
        echo "Release contract is valid ($VERSION)."
        exit 0
        ;;
    start-development)
        [[ -n "$VERSION" ]] || fail "--start-development requires X.Y.Z-dev"
        if [[ "$DRY_RUN" != true ]]; then assert_clean_tree; fi
        start_development "$VERSION" "$DRY_RUN"
        exit 0
        ;;
esac

[[ -n "$VERSION" ]] || fail "release version is required"
validate_version "$VERSION" release

if [[ "$SEAL_ONLY" == true ]]; then
    prepare_release_contract "$VERSION" "$DRY_RUN"
    exit 0
fi

assert_clean_tree
mapfile -t REMOTES < <(git remote)
[[ ${#REMOTES[@]} -gt 0 ]] || fail "no Git remotes are configured"
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
CURRENT_VERSION=$(workspace_version "$CARGO_TOML")
TAG_NAME="v${VERSION}"
MESSAGE="${MESSAGE:-release: v${VERSION}}"

echo "Current branch: $CURRENT_BRANCH"
echo "Cargo version: $CURRENT_VERSION -> $VERSION"
echo "Tag: $TAG_NAME"
echo "Remotes: ${REMOTES[*]}"

if [[ "$DRY_RUN" == true ]]; then
    prepare_release_contract "$VERSION" true
    echo "[DRY RUN] Would commit, tag $TAG_NAME, and push to: ${REMOTES[*]}"
    exit 0
fi

read -r -p "Proceed with release v${VERSION}? [y/N] " CONFIRM
if [[ ! "$CONFIRM" =~ ^[yY] ]]; then
    echo "Aborted."
    exit 0
fi

prepare_release_contract "$VERSION" false
git add Cargo.toml release/breaking-changes.md
git commit -m "$MESSAGE"
git tag -a "$TAG_NAME" -m "$MESSAGE"

if [[ "$NO_PUSH" != true ]]; then
    for remote in "${REMOTES[@]}"; do
        git push "$remote" "$CURRENT_BRANCH"
        git push "$remote" "$TAG_NAME"
        echo "${remote}: pushed ${CURRENT_BRANCH} + ${TAG_NAME}"
    done
else
    echo "Skipped push (--no-push). Commit and tag are local only."
fi
