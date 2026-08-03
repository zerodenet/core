#!/usr/bin/env bash
#
# Manage the Zero version contract and release lifecycle.
#
# Version policy:
#   X.Y.Z-dev.N -> X.Y.Z-alpha.N -> X.Y.Z-beta.N -> X.Y.Z-rc.N -> X.Y.Z
#
# Common commands:
#   ./scripts/release.sh --check
#   ./scripts/release.sh --next rc
#   ./scripts/release.sh --check-transition origin/main HEAD
#   ./scripts/release.sh --verify-tag v0.0.16-rc.1
#   ./scripts/release.sh 0.0.16-rc.1 --seal-only
#   ./scripts/release.sh 0.0.17-dev.1 --start-development
set -euo pipefail

MODE=release
DRY_RUN=false
NO_PUSH=false
SEAL_ONLY=false
ALLOW_GAP=false
MESSAGE=""
VERSION=""
BASE_REF=""
HEAD_REF="HEAD"
NEXT_STAGE=""
BUMP="patch"
REMOTE="origin"
TAG_NAME=""

usage() {
    cat <<'USAGE'
Usage:
  release.sh --check
  release.sh --next <dev|alpha|beta|rc|stable> [--bump patch|minor|major]
  release.sh --check-transition <base-ref> [head-ref]
  release.sh --verify-tag <vX.Y.Z[-stage.N]>
  release.sh <version> --check-release
  release.sh <version> --start-development [--dry-run]
  release.sh <version> --seal-only [--dry-run]
  release.sh <version> [--dry-run] [--no-push] [--remote origin]

Accepted versions:
  X.Y.Z-dev.N
  X.Y.Z-alpha.N
  X.Y.Z-beta.N
  X.Y.Z-rc.N
  X.Y.Z
USAGE
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --check) MODE=check; shift ;;
        --check-release) MODE=check-release; shift ;;
        --check-transition)
            MODE=check-transition
            [[ $# -ge 2 ]] || usage
            BASE_REF=$2
            shift 2
            if [[ $# -gt 0 && "$1" != -* ]]; then HEAD_REF=$1; shift; fi
            ;;
        --verify-tag)
            MODE=verify-tag
            [[ $# -ge 2 ]] || usage
            TAG_NAME=$2
            shift 2
            ;;
        --next)
            MODE=next
            [[ $# -ge 2 ]] || usage
            NEXT_STAGE=$2
            shift 2
            ;;
        --bump)
            [[ $# -ge 2 ]] || usage
            BUMP=$2
            shift 2
            ;;
        --start-development) MODE=start-development; shift ;;
        --dry-run) DRY_RUN=true; shift ;;
        --no-push) NO_PUSH=true; shift ;;
        --seal-only) SEAL_ONLY=true; shift ;;
        --allow-gap) ALLOW_GAP=true; shift ;;
        --remote)
            [[ $# -ge 2 ]] || usage
            REMOTE=$2
            shift 2
            ;;
        -m|--message)
            [[ $# -ge 2 ]] || usage
            MESSAGE=$2
            shift 2
            ;;
        --help|-h) usage ;;
        -*) echo "Unknown option: $1" >&2; usage ;;
        *)
            if [[ -n "$VERSION" ]]; then
                echo "Version already set to '$VERSION', unexpected argument: $1" >&2
                usage
            fi
            VERSION=$1
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

V_MAJOR=0
V_MINOR=0
V_PATCH=0
V_STAGE=stable
V_SEQ=0

parse_version() {
    local version=$1
    if [[ "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
        V_MAJOR=${BASH_REMATCH[1]}
        V_MINOR=${BASH_REMATCH[2]}
        V_PATCH=${BASH_REMATCH[3]}
        V_STAGE=stable
        V_SEQ=0
        return 0
    fi
    if [[ "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)-(dev|alpha|beta|rc)\.([1-9][0-9]*)$ ]]; then
        V_MAJOR=${BASH_REMATCH[1]}
        V_MINOR=${BASH_REMATCH[2]}
        V_PATCH=${BASH_REMATCH[3]}
        V_STAGE=${BASH_REMATCH[4]}
        V_SEQ=${BASH_REMATCH[5]}
        return 0
    fi
    return 1
}

validate_version() {
    local version=$1 expected=${2:-any}
    parse_version "$version" || \
        fail "invalid version '$version'; expected X.Y.Z or X.Y.Z-(dev|alpha|beta|rc).N"
    case "$expected" in
        any) ;;
        development)
            [[ "$V_STAGE" == dev ]] || fail "development version must use '-dev.N'"
            ;;
        release)
            [[ "$V_STAGE" != dev ]] || fail "release version must not use the dev stage"
            ;;
        stable)
            [[ "$V_STAGE" == stable ]] || fail "stable version must use X.Y.Z without a suffix"
            ;;
        *) fail "unknown version expectation '$expected'" ;;
    esac
}

stage_rank() {
    case "$1" in
        dev) echo 0 ;;
        alpha) echo 1 ;;
        beta) echo 2 ;;
        rc) echo 3 ;;
        stable) echo 4 ;;
        *) return 1 ;;
    esac
}

version_key() {
    local version=$1 rank
    parse_version "$version" || return 1
    rank=$(stage_rank "$V_STAGE")
    printf '%010d%010d%010d%02d%010d\n' "$V_MAJOR" "$V_MINOR" "$V_PATCH" "$rank" "$V_SEQ"
}

compare_versions() {
    local left=$1 right=$2 left_key right_key
    left_key=$(version_key "$left") || fail "cannot compare invalid version '$left'"
    right_key=$(version_key "$right") || fail "cannot compare invalid version '$right'"
    if [[ "$left_key" < "$right_key" ]]; then echo -1
    elif [[ "$left_key" > "$right_key" ]]; then echo 1
    else echo 0
    fi
}

same_base() {
    local left=$1 right=$2 lm lmi lp rm rmi rp
    parse_version "$left" || return 1
    lm=$V_MAJOR; lmi=$V_MINOR; lp=$V_PATCH
    parse_version "$right" || return 1
    rm=$V_MAJOR; rmi=$V_MINOR; rp=$V_PATCH
    [[ "$lm" == "$rm" && "$lmi" == "$rmi" && "$lp" == "$rp" ]]
}

assert_transition() {
    local from=$1 to=$2 cmp from_stage from_seq from_rank to_stage to_seq to_rank
    validate_version "$from" any
    from_stage=$V_STAGE; from_seq=$V_SEQ; from_rank=$(stage_rank "$V_STAGE")
    validate_version "$to" any
    to_stage=$V_STAGE; to_seq=$V_SEQ; to_rank=$(stage_rank "$V_STAGE")

    cmp=$(compare_versions "$from" "$to")
    [[ "$cmp" -lt 0 ]] || fail "version must move forward: $from -> $to"

    if same_base "$from" "$to"; then
        [[ "$from_stage" != stable ]] || fail "stable version '$from' is immutable"
        [[ "$to_rank" -ge "$from_rank" ]] || \
            fail "release stage must not move backward: $from -> $to"

        if [[ "$to_stage" == "$from_stage" ]]; then
            [[ "$to_stage" != stable ]] || fail "stable version '$to' already exists"
            if [[ "$ALLOW_GAP" == true ]]; then
                [[ "$to_seq" -gt "$from_seq" ]] || fail "stage number must increase: $from -> $to"
            else
                [[ "$to_seq" -eq $((from_seq + 1)) ]] || \
                    fail "stage number must be consecutive: expected $((from_seq + 1)), got $to_seq"
            fi
        elif [[ "$to_stage" == stable ]]; then
            [[ "$from_stage" == rc ]] || fail "stable release requires a prior rc version"
        else
            [[ "$to_seq" -eq 1 ]] || \
                fail "a new release stage must start at .1: $from -> $to"
        fi
    else
        [[ "$to_stage" != stable ]] || \
            fail "a new base version must enter through a prerelease stage before stable"
        [[ "$to_seq" -eq 1 ]] || \
            fail "a new base version must start its stage at .1: $from -> $to"
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

workspace_version_at_ref() {
    local ref=$1 content
    content=$(git show "${ref}:Cargo.toml" 2>/dev/null) || fail "cannot read Cargo.toml at '$ref'"
    printf '%s\n' "$content" | awk '
        /^\[workspace\.package\][[:space:]]*$/ { in_workspace=1; next }
        in_workspace && /^\[/ { exit }
        in_workspace && /^version[[:space:]]*=/ {
            if (match($0, /"[^"]+"/)) {
                print substr($0, RSTART + 1, RLENGTH - 2)
                exit
            }
        }
    '
}

strict_tag_versions() {
    git tag --list 'v*' 2>/dev/null | while IFS= read -r tag; do
        local_version=${tag#v}
        if parse_version "$local_version"; then
            printf '%s\n' "$local_version"
        fi
    done
}

latest_strict_version() {
    local exclude=${1:-} latest="" candidate
    while IFS= read -r candidate; do
        [[ -n "$candidate" ]] || continue
        [[ "$candidate" != "$exclude" ]] || continue
        if [[ -z "$latest" || "$(compare_versions "$candidate" "$latest")" -gt 0 ]]; then
            latest=$candidate
        fi
    done < <(strict_tag_versions)
    printf '%s\n' "$latest"
}

assert_history_transition() {
    local target=$1 latest
    latest=$(latest_strict_version "$target")
    if [[ -n "$latest" && "$latest" != "$target" ]]; then
        assert_transition "$latest" "$target"
    fi
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
    local cargo=${1:-$CARGO_TOML} breaking=${2:-$BREAKING_CHANGES} current
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
    local cargo=${1:-$CARGO_TOML} breaking=${2:-$BREAKING_CHANGES} current
    current=$(workspace_version "$cargo")
    [[ -n "$current" ]] || fail "workspace package version was not found"
    validate_version "$current" any
    if [[ "$V_STAGE" == dev ]] && grep -Fq "$current" "$breaking"; then
        fail "development version '$current' must not be bound into the compatibility ledger"
    fi
    assert_common_contract "$breaking"
    echo "$current"
}

assert_release_contract() {
    local cargo=$1 breaking=$2 release_version=$3 current row
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
    local release_version=$1 dry_run=$2 current cargo_tmp docs_tmp has_changes
    current=$(assert_unsealed_contract "$CARGO_TOML" "$BREAKING_CHANGES")
    validate_version "$release_version" release
    assert_history_transition "$release_version"
    if [[ "$current" != "$release_version" ]]; then
        assert_transition "$current" "$release_version"
    fi
    if grep -Eq "^## ${release_version//./\\.}\r?$" "$BREAKING_CHANGES"; then
        fail "release '$release_version' already exists in breaking changes"
    fi
    has_changes=false
    if unreleased_body "$BREAKING_CHANGES" | body_is_substantive; then has_changes=true; fi
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
    validate_version "$current" any
    assert_common_contract "$BREAKING_CHANGES"
    assert_history_transition "$version"
    if [[ "$current" != "$version" ]]; then assert_transition "$current" "$version"; fi
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

bump_base() {
    local version=$1 bump=$2
    parse_version "$version" || fail "cannot bump invalid version '$version'"
    case "$bump" in
        patch) printf '%d.%d.%d\n' "$V_MAJOR" "$V_MINOR" $((V_PATCH + 1)) ;;
        minor) printf '%d.%d.0\n' "$V_MAJOR" $((V_MINOR + 1)) ;;
        major) printf '%d.0.0\n' $((V_MAJOR + 1)) ;;
        *) fail "invalid bump '$bump'; expected patch, minor, or major" ;;
    esac
}

next_version() {
    local stage=$1 bump=$2 current current_stage current_seq current_base target_base current_rank target_rank
    case "$stage" in dev|alpha|beta|rc|stable) ;; *) fail "invalid stage '$stage'" ;; esac
    current=$(workspace_version "$CARGO_TOML")
    validate_version "$current" any
    current_stage=$V_STAGE; current_seq=$V_SEQ
    current_base="$V_MAJOR.$V_MINOR.$V_PATCH"
    current_rank=$(stage_rank "$current_stage")
    target_rank=$(stage_rank "$stage")

    if [[ "$current_stage" == stable ]]; then
        [[ "$stage" != stable ]] || fail "cannot calculate a new stable version without a prerelease cycle"
        target_base=$(bump_base "$current" "$bump")
        printf '%s-%s.1\n' "$target_base" "$stage"
        return
    fi

    [[ "$target_rank" -ge "$current_rank" ]] || \
        fail "release stage must not move backward: $current_stage -> $stage"
    if [[ "$stage" == "$current_stage" ]]; then
        printf '%s-%s.%d\n' "$current_base" "$stage" $((current_seq + 1))
    elif [[ "$stage" == stable ]]; then
        [[ "$current_stage" == rc ]] || fail "stable release requires a prior rc version"
        printf '%s\n' "$current_base"
    else
        printf '%s-%s.1\n' "$current_base" "$stage"
    fi
}

assert_clean_tree() {
    [[ -z "$(git status --porcelain)" ]] || \
        fail "working tree is not clean; commit or stash changes before changing versions"
}

verify_tag() {
    local tag=$1 version current branch
    [[ "$tag" == v* ]] || fail "release tag must start with 'v'"
    version=${tag#v}
    validate_version "$version" any
    current=$(workspace_version "$CARGO_TOML")
    [[ "$current" == "$version" ]] || fail "tag '$tag' does not match Cargo version '$current'"
    assert_history_transition "$version"
    if [[ "$V_STAGE" == dev ]]; then
        assert_development_contract "$CARGO_TOML" "$BREAKING_CHANGES" >/dev/null
    else
        assert_release_contract "$CARGO_TOML" "$BREAKING_CHANGES" "$version"
    fi
    if git rev-parse --verify HEAD >/dev/null 2>&1; then
        branch=$(git branch -r --contains HEAD 2>/dev/null | sed 's/^[ *]*//' || true)
        if [[ -n "$branch" && "$branch" != *"origin/main"* ]]; then
            fail "release tags must point to a commit contained in origin/main"
        fi
    fi
    echo "Release tag is valid ($tag)."
}

require_files

case "$MODE" in
    check)
        current=$(workspace_version "$CARGO_TOML")
        validate_version "$current" any
        if [[ "$V_STAGE" == dev ]]; then
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
        assert_history_transition "$VERSION"
        echo "Release contract is valid ($VERSION)."
        exit 0
        ;;
    check-transition)
        [[ -n "$BASE_REF" ]] || fail "--check-transition requires a base ref"
        from=$(workspace_version_at_ref "$BASE_REF")
        to=$(workspace_version_at_ref "$HEAD_REF")
        [[ -n "$from" && -n "$to" ]] || fail "workspace version was not found in one of the refs"
        if [[ "$from" == "$to" ]]; then
            echo "Version is unchanged ($to)."
        else
            assert_transition "$from" "$to"
            echo "Version transition is valid ($from -> $to)."
        fi
        exit 0
        ;;
    verify-tag)
        [[ -n "$TAG_NAME" ]] || fail "--verify-tag requires a tag"
        verify_tag "$TAG_NAME"
        exit 0
        ;;
    next)
        [[ -n "$NEXT_STAGE" ]] || fail "--next requires a stage"
        next_version "$NEXT_STAGE" "$BUMP"
        exit 0
        ;;
    start-development)
        [[ -n "$VERSION" ]] || fail "--start-development requires X.Y.Z-dev.N"
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
[[ -n "$(git remote get-url "$REMOTE" 2>/dev/null || true)" ]] || \
    fail "Git remote '$REMOTE' is not configured"
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
CURRENT_VERSION=$(workspace_version "$CARGO_TOML")
TAG_NAME="v${VERSION}"
MESSAGE="${MESSAGE:-release: v${VERSION}}"

echo "Current branch: $CURRENT_BRANCH"
echo "Cargo version: $CURRENT_VERSION -> $VERSION"
echo "Tag: $TAG_NAME"
echo "Remote: $REMOTE"

if [[ "$DRY_RUN" == true ]]; then
    prepare_release_contract "$VERSION" true
    echo "[DRY RUN] Would commit, tag $TAG_NAME, and push to $REMOTE"
    exit 0
fi

read -r -p "Proceed with release v${VERSION}? [y/N] " CONFIRM
if [[ ! "$CONFIRM" =~ ^[yY] ]]; then echo "Aborted."; exit 0; fi

prepare_release_contract "$VERSION" false
git add Cargo.toml release/breaking-changes.md
git commit -m "$MESSAGE"
git tag -a "$TAG_NAME" -m "$MESSAGE"

if [[ "$NO_PUSH" != true ]]; then
    git push "$REMOTE" "$CURRENT_BRANCH"
    git push "$REMOTE" "$TAG_NAME"
    echo "$REMOTE: pushed $CURRENT_BRANCH + $TAG_NAME"
else
    echo "Skipped push (--no-push). Commit and tag are local only."
fi
