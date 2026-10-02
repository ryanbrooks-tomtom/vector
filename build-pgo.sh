#!/usr/bin/env bash
# Profile-guided (PGO) Vector image for linux/arm64, in two passes around an
# in-cluster capture (observability-dev-test SPSRE-4775;
# o11y-proxy/docs/vector-pgo-evaluation-plan.md).
#
#   ./build-pgo.sh instrumented      pass 1: -Cprofile-generate image for the capture Job
#   ./build-pgo.sh fetch <raw-dir>   copy each stopped pod's .profraw out of the cluster
#   ./build-pgo.sh merge <raw-dir>   merge the copied .profraw files into pgo/merged.profdata
#   ./build-pgo.sh optimized         pass 2: -Cprofile-use image from pgo/merged.profdata
#
# Each pass takes about 30 minutes on 16 cores. The optimized pass refuses to
# build unless the source matches the commit the profile was captured from.
# The full procedure is in PGO.md.
set -euo pipefail

IMAGE_REPO=artifactory.tomtomgroup.com/docker-dev/timberio/vector
TARGET=aarch64-unknown-linux-musl

# Cross.toml mounts this directory at the same path inside the container.
export VECTOR_PGO_DIR="$PWD/pgo"
PROFILE="$VECTOR_PGO_DIR/merged.profdata"
INSTRUMENTED_COMMIT="$VECTOR_PGO_DIR/instrumented.commit"
PROFILE_COMMIT="$VECTOR_PGO_DIR/merged.commit"

# llvm-profdata must come from the toolchain that compiles both passes.
TOOLCHAIN="$(sed -n 's/^channel = "\(.*\)"/\1/p' rust-toolchain.toml)"
PROFDATA="$HOME/.rustup/toolchains/${TOOLCHAIN}-x86_64-unknown-linux-gnu/lib/rustlib/x86_64-unknown-linux-gnu/bin/llvm-profdata"

VERSION="$(cargo vdev version)"
ARCHIVE="target/artifacts/vector-${VERSION}-${TARGET}.tar.gz"

die() { printf 'build-pgo: %s\n' "$*" >&2; exit 1; }

# Build tooling may differ from the profiled commit; Rust source and
# Cargo.lock may not.
BUILD_TOOLING=(
  ':(exclude)Cross.toml'
  ':(exclude)PGO.md'
  ':(exclude)build.sh'
  ':(exclude)build-pgo.sh'
  ':(exclude).gitignore'
)

source_matches() {
  git diff --quiet "$1" -- . "${BUILD_TOOLING[@]}"
}

preflight() {
  grep -q VECTOR_PGO_DIR Cross.toml ||
    die "Cross.toml lacks volumes = [\"VECTOR_PGO_DIR\"]"
  grep -q CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_RUSTFLAGS Cross.toml ||
    die "Cross.toml does not pass through CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_RUSTFLAGS"
  mkdir -p "$VECTOR_PGO_DIR"
}

# $1 = extra rustflags, $2 = build log
build_package() {
  # The Cursor shell points this at a sandbox cache, which breaks packaging.
  unset CARGO_TARGET_DIR
  # make skips the build when the archive already exists.
  rm -f "$ARCHIVE"
  CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_RUSTFLAGS="-Lnative=/lib/native-libs $1" \
    CONTAINER_TOOL=docker CROSS_CONTAINER_ENGINE=docker \
    make "package-${TARGET}-all" 2>&1 | tee "$2"
  test -f "$ARCHIVE"
}

# $1 = tag variant (pgo-instrumented or pgo)
build_image() {
  local image="${IMAGE_REPO}:${VERSION}-$1-alpine-arm64"
  local context="target/artifacts/docker-alpine-${VERSION}-$1-arm64"

  cp "$ARCHIVE" "$ARCHIVE.$1"
  rm -rf "$context"
  mkdir -p "$context"
  cp "$ARCHIVE" "$context/"

  docker run --privileged --rm tonistiigi/binfmt --install arm64 >/dev/null
  docker build \
    --platform linux/arm64 \
    --provenance=false \
    --file distribution/docker/alpine/Dockerfile \
    --tag "$image" \
    "$context"
  docker run --rm --platform linux/arm64 "$image" --version

  docker login artifactory.tomtomgroup.com
  docker push "$image"
  docker buildx imagetools inspect "$image" | head -n 4
}

instrumented() {
  preflight
  source_matches HEAD ||
    die "uncommitted source changes; the profile must map to a commit"
  build_package "-Cprofile-generate=/tmp/pgo" "$VECTOR_PGO_DIR/build-instrumented.log"
  git rev-parse HEAD >"$INSTRUMENTED_COMMIT"
  build_image pgo-instrumented
}

# kubectl cp truncated files silently, so stream with cat and compare sha256.
fetch() {
  local raw="${1:?usage: build-pgo.sh fetch <new dir for .profraw files>}"
  local ns pod f out want got copied=0
  mkdir -p "$raw"
  for ns in o11y-proxy-data-ingress o11y-proxy-data-egress; do
    for pod in $(kubectl get pods -n "$ns" -o name | grep vector | cut -d/ -f2); do
      kubectl exec -n "$ns" "$pod" -c vector -- sh -c '! pidof vector >/dev/null' ||
        die "$ns/$pod: Vector is still running; set stop: \"true\" in pgo-control and wait"
      for f in $(kubectl exec -n "$ns" "$pod" -c vector -- sh -c 'ls /pgo/*.profraw'); do
        out="$raw/$(basename "$f")"
        want="$(kubectl exec -n "$ns" "$pod" -c vector -- sha256sum "$f" | cut -d' ' -f1)"
        got=""
        for _ in 1 2 3 4 5 6; do
          kubectl exec -n "$ns" "$pod" -c vector -- cat "$f" >"$out"
          got="$(sha256sum "$out" | cut -d' ' -f1)"
          [ "$got" = "$want" ] && break
        done
        [ "$got" = "$want" ] || die "$ns/$pod: $f did not copy intact in 6 attempts"
        copied=$((copied + 1))
      done
    done
  done
  printf '%s profiles copied to %s (expect 9: 6 ingress, 3 egress)\n' "$copied" "$raw"
}

merge() {
  local raw="${1:?usage: build-pgo.sh merge <dir with .profraw files>}"
  test -s "$INSTRUMENTED_COMMIT" || die "no $INSTRUMENTED_COMMIT; run the instrumented pass first"
  test -x "$PROFDATA" || die "missing $PROFDATA; run: rustup component add llvm-tools --toolchain $TOOLCHAIN"
  "$PROFDATA" merge -o "$PROFILE" "$raw"/*.profraw
  cp "$INSTRUMENTED_COMMIT" "$PROFILE_COMMIT"
  "$PROFDATA" show "$PROFILE" | tail -n 4
  sha256sum "$PROFILE"
}

optimized() {
  preflight
  test -s "$PROFILE" || die "no $PROFILE; run the merge step first"
  test -s "$PROFILE_COMMIT" || die "no $PROFILE_COMMIT; cannot tell which commit $PROFILE belongs to"
  local commit
  commit="$(cat "$PROFILE_COMMIT")"
  source_matches "$commit" ||
    die "source differs from the profiled commit ${commit:0:10}; capture a new profile (o11y-proxy/docs/vector-pgo-profile-regeneration.md)"

  local log="$VECTOR_PGO_DIR/build-optimized.log"
  build_package "-Cprofile-use=$PROFILE -Cllvm-args=-pgo-warn-missing-function" "$log"

  # Only Vector's workspace crates report these; Cargo caps crates.io lints.
  local mismatched missing
  mismatched="$(grep -c 'hash mismatch' "$log" || true)"
  missing="$(grep -c 'no profile data available' "$log" || true)"
  printf 'profile %s (commit %s): %s hash mismatches, %s functions without profile data\n' \
    "$(sha256sum "$PROFILE" | cut -c1-12)" "${commit:0:10}" "$mismatched" "$missing"
  [ "$mismatched" -eq 0 ] || die "profile does not match the compiled source; see $log"

  build_image pgo
}

case "${1:-}" in
  instrumented) instrumented ;;
  fetch) fetch "${2:-}" ;;
  merge) merge "${2:-}" ;;
  optimized) optimized ;;
  *) die "usage: build-pgo.sh instrumented | fetch <raw-dir> | merge <raw-dir> | optimized" ;;
esac
