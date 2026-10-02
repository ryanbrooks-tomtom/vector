set -euo pipefail

BRANCH=cursor/nats-end-to-end-ack-v0-58-0-3111
IMAGE_REPO=artifactory.tomtomgroup.com/docker-dev/timberio/vector

# 1. Check out the backport.
git fetch origin "$BRANCH"
git switch "$BRANCH" 2>/dev/null ||
  git switch --track -c "$BRANCH" "origin/$BRANCH"
git pull --ff-only origin "$BRANCH"

# 2. Verify prerequisites and install cross if necessary.
docker version
cargo --version
command -v cross >/dev/null ||
  cargo install cross --version 0.2.5 --locked

# 3. Build the Linux/amd64 musl package.
CONTAINER_TOOL=docker CROSS_CONTAINER_ENGINE=docker \
  make package-x86_64-unknown-linux-musl-all

VERSION="$(cargo vdev version)"
ARCHIVE="target/artifacts/vector-${VERSION}-x86_64-unknown-linux-musl.tar.gz"
test -f "$ARCHIVE"

# 4. Build the Alpine image using an isolated context.
BUILD_CONTEXT="target/artifacts/docker-alpine-${VERSION}"
mkdir -p "$BUILD_CONTEXT"
cp "$ARCHIVE" "$BUILD_CONTEXT/"

IMAGE="${IMAGE_REPO}:${VERSION}-alpine"

docker build \
  --platform linux/amd64 \
  --file distribution/docker/alpine/Dockerfile \
  --tag "$IMAGE" \
  "$BUILD_CONTEXT"

# 5. Verify the local image.
docker image inspect "$IMAGE" \
  --format 'tag={{join .RepoTags ","}} architecture={{.Architecture}}'
docker run --rm "$IMAGE" --version

# 6. Authenticate and push.
docker login artifactory.tomtomgroup.com
docker push "$IMAGE"

# 7. Confirm it can be pulled.
docker pull "$IMAGE"

printf '\nKubernetes image:\n  repository: %s\n  tag: %s-alpine\n' \
  "$IMAGE_REPO" "$VERSION"
