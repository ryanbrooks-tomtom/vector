# Profile-guided (PGO) Vector build

As of 2026-10-02. How to capture a new profile and build the PGO image that
o11y-proxy dev runs (Jira SPSRE-4775). Everything below was done once for
`0.58.0.custom.5bd4ebf8b4`; every command and YAML block is the one that
worked.

- **This repo** (`~/develop/_external/vector`): `build-pgo.sh`, `Cross.toml`,
  and the `pgo/` working directory.
- **observability-dev-test** (`~/develop/observability-dev-test`): the
  GitOps commits that run the capture in the cluster, the result YAMLs, and
  the docs. Every cluster change there goes through a commit; nothing is
  applied by hand.

## When to do this

Read `o11y-proxy/docs/vector-pgo-profile-regeneration.md` in
observability-dev-test. In short:

- **New profile required:** any change to Vector source, `Cargo.lock`, or
  `rust-toolchain.toml` (version bump, patch rebase, new patch).
  `build-pgo.sh optimized` refuses to build otherwise.
- **New profile recommended:** the o11y-proxy config starts using a new
  component type, codec, compression, or a VRL function that runs per event.
- **No new profile:** edits to existing VRL, routing, thresholds,
  partitions, replicas, and NATS settings.

## Time budget

About 3 hours of wall time, mostly waiting:

| Step | Time |
|---|---|
| Pass 1, instrumented build and push | 35 minutes |
| Capture commits, rollout, 20-minute run, stop | 50 minutes |
| Fetch and merge | 5 minutes |
| Pass 2, PGO build and push | 35 minutes |
| Unit tests, pin, rollout check | 20 minutes |
| A/B profile run (version bumps only) | 90 minutes |

## Prerequisites

1. Toolchain from `rust-toolchain.toml` (1.95 as of 2026-10-02) plus
   `llvm-tools`; `llvm-profdata` must come from that toolchain:

   ```bash
   rustup component add llvm-tools --toolchain 1.95
   ```

2. `cross` 0.2.5 (`cargo install cross --version 0.2.5 --locked`), Docker,
   and `docker login artifactory.tomtomgroup.com`.
3. VPN on, `kubectl` pointing at the dev AKS cluster.
4. `Cross.toml` carries two additions (committed on this branch; carry them
   across any rebase):

   ```toml
   [build.env]
   passthrough = [
       # ... existing entries ...
       "CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_RUSTFLAGS",
   ]
   # Mounted at the same path in the container, so a `-Cprofile-use=` path also
   # resolves for cross's host-side `cargo metadata` call.
   volumes = ["VECTOR_PGO_DIR"]
   ```

   Why: `.cargo/config.toml` sets `-Lnative=/lib/native-libs` for the musl
   target. A plain `RUSTFLAGS` replaces it and breaks the libstdc++ link, so
   the PGO flags go in the target-specific variable, which repeats that flag.
   `cross` runs `cargo metadata` on the host before building in the container
   (source at `/project`), so the profile path must exist at the same
   absolute path in both; the volume entry does that.

`build-pgo.sh` checks the `Cross.toml` entries, unsets `CARGO_TARGET_DIR`
(the Cursor shell points it at a sandbox cache and packaging then fails),
deletes the archive before each build (`make` skips the build when the archive
exists, whatever the flags), and registers QEMU for the arm64 `docker build`.

## `pgo/` layout

| Path | Contents |
|---|---|
| `pgo/raw-rN/` | `.profraw` files from capture N, one per pod (`<hostname>-<pid>.profraw`) |
| `pgo/merged.profdata` | the profile pass 2 uses |
| `pgo/merged.commit` | Vector commit `merged.profdata` was captured from |
| `pgo/instrumented.commit` | commit of the last instrumented build |
| `pgo/build-*.log` | full build logs |

Keep old `raw-rN/` directories until the new image is adopted.

## Step 1: instrumented image

```bash
cd ~/develop/_external/vector
git status   # source must be committed; only build tooling may differ
./build-pgo.sh instrumented
```

This builds with `-Lnative=/lib/native-libs -Cprofile-generate=/tmp/pgo`,
pushes `<version>-pgo-instrumented-alpine-arm64`, and prints the digest.
Record the digest for Commit A. The binary writes its profile only when it
exits; `LLVM_PROFILE_FILE` (set in Commit A) overrides the `/tmp/pgo` path.

## Step 2: capture in the cluster

All in observability-dev-test, as commits the owner pushes. Use the next free
run number `N` (r1 and r2 exist). Check each push with
`flux get kustomizations o11y-proxy` and the pod images before going on.

### Commit A: instrumented Vector and a suspended capture Job

In both `o11y-proxy/data-ingress/values.yaml` and
`o11y-proxy/data-egress/values.yaml`:

```yaml
image:
  # PGO profile capture only: instrumented, arm64-only image.
  tag: <version>-pgo-instrumented-alpine-arm64   # no @sha256 in tag
  sha: sha256:<instrumented digest>

podAnnotations:
  # ... existing entries ...
  # A new value restarts Vector with fresh profile counters.
  o11y-dev.tomtomgroup.com/pgo-capture: rN

# The instrumented binary writes its profile at exit. Adding `stop` to the
# pgo-control ConfigMap stops Vector gracefully; the container then idles so
# the profile can be copied out.
command: [/bin/sh, -c]
args:
  - |
    /usr/local/bin/vector --config-dir /etc/vector/ &
    pid=$!
    trap 'kill -TERM "$pid"' TERM
    until [ -e /pgo-control/stop ]; do
      kill -0 "$pid" 2>/dev/null || { wait "$pid"; exit $?; }
      sleep 10
    done
    kill -TERM "$pid"
    wait "$pid"
    ls -l /pgo
    trap 'exit 0' TERM
    while true; do sleep 5; done

env:   # ingress has `env: []`; replace it
  - name: LLVM_PROFILE_FILE
    value: /pgo/%h-%p.profraw

extraVolumes:
  # ... existing entries ...
  - name: pgo
    emptyDir: {}
  - name: pgo-control
    configMap:
      name: pgo-control

extraVolumeMounts:
  # ... existing entries ...
  - name: pgo
    mountPath: /pgo
  # No subPath, so a ConfigMap change reaches running pods.
  - name: pgo-control
    mountPath: /pgo-control
    readOnly: true
```

The consolidated egress releases inherit these values, so the two files cover
all 9 pods (6 ingress, 3 egress).

Add `pgo-control.yaml` in both `data-ingress/` and `data-egress/`, and list
it under `resources:` in each `kustomization.yaml`. It is a plain resource,
not a generator, so its name is fixed and changing it does not restart pods:

```yaml
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: pgo-control
data: {}
```

In `o11y-proxy/test-suite/avalanche/vector-pgo-profile-capture-job.yaml`,
copy the `vector-pgo-profile-capture-r2` Job to `-rN`, change `metadata.name`
and both `CAPACITY_RUN` values, and keep `suspend: true`. Its load is the
`architecture-performance-profile-prodshape-r1` generators (Avalanche plus
`prodshape_extras.py`, Basic/JWT mix) without the backlog controller, at 10%
of sustained:

```yaml
- name: STAGES
  value: tenth:740:71.82608s:17          # 17 x 71.8 s = about 20 minutes
- {name: STAGGER_STEP_SECONDS, value: "4.48913"}   # interval / 16
- {name: TARGET_SAMPLES_PER_SECOND, value: "105669"}
- {name: TARGET_REQUESTS_PER_SECOND, value: "142.887"}
```

Do not raise the rate. The instrumented binary needs several times the
release CPU: at quarter rate (r1) egress was throttled in 45% of periods and
pending reached 529,000, which weights the profile toward backlog and retry
code. At 10% (r2) ingress used 36 cores unthrottled and egress kept pace.

Before reporting Commit A, render it: `kubectl kustomize o11y-proxy`, and
`python3 .cursor/skills/o11y-proxy-render-diff/scripts/render_diff.py`; the
Helm diff shows the image, the wrapper, the env, the volumes, and the
annotation.

After the push, wait until all 9 pods run the instrumented digest and are
Ready (about 5 minutes):

```bash
for ns in o11y-proxy-data-ingress o11y-proxy-data-egress; do
  kubectl get pods -n "$ns" -o custom-columns='NAME:.metadata.name,READY:.status.containerStatuses[0].ready,IMAGE:.spec.containers[0].image'
done
```

### Preflight

1. NATS leaders balanced (see `AGENTS.md` in observability-dev-test; ask the
   owner before any step-down):
   `kubectl exec -n o11y-proxy-queue deploy/nats-server-box -- nats stream report`
2. Pending envelopes at 0 in the same report.

### Commit B: start

Set `suspend: false` on the `-rN` Job. Watch for about 20 minutes:

- `kubectl get job -n o11y-proxy-test vector-pgo-profile-capture-rN`
- `kubectl top pods -n o11y-proxy-data-ingress` and `-n o11y-proxy-data-egress`
- pending envelopes in `nats stream report`: bounded and returning to near
  zero between sweeps (r2 peaked at 71,000)
- generator logs: zero errors

Queries for CPU and throttling are in the `o11y-proxy-capacity-run` skill.

### Commit C: stop Vector and re-suspend

When the Job is Complete (16/16) and pending is 0, set `stop: "true"` in both
`pgo-control.yaml` files and `suspend: true` on the Job:

```yaml
data:
  stop: "true"
```

The kubelet takes up to about 2 minutes to update the mounted ConfigMap; the
wrapper polls every 10 seconds, stops Vector, and lists `/pgo` in the pod log.
Wait until every pod's log shows that listing (each file about 63 MB):

```bash
kubectl logs -n o11y-proxy-data-ingress <pod> -c vector --tail 3
```

The Vector pods have no readiness or liveness probe, so they stay Running and
Ready although Vector has exited: the pipeline is down until Commit D, and
nothing in `kubectl get pods` shows it.

### Fetch

```bash
cd ~/develop/_external/vector
./build-pgo.sh fetch pgo/raw-rN
```

`fetch` refuses any pod where Vector still runs, then streams each
`/pgo/*.profraw` with `kubectl exec ... cat` and compares it with the
in-pod `sha256sum`, retrying up to six times. Do not use `kubectl cp`: it
silently truncated files in r1. Expect `9 profiles copied`. The commands are
read-only, but confirm with the owner before running them.

### Commit D: restore the standing image

Revert Commit A's values changes, putting back the image that ran before the
capture (the current PGO image, not the release image). Remove both
`pgo-control.yaml` files and their `kustomization.yaml` entries. Keep the
`-rN` Job, suspended. `render_diff.py <commit before A>` must show no
difference outside the Job file.

The capture leaves data in the NATS streams; it expires under the 2-hour
`maxAge`. Start any A/B run after that, so it begins on empty streams.

## Step 3: merge

```bash
./build-pgo.sh merge pgo/raw-rN
```

Writes `pgo/merged.profdata`, copies `pgo/instrumented.commit` to
`pgo/merged.commit`, and prints the totals and the sha256. Ingress and egress
counts add, so no weighting is needed. Merge only one capture: do not mix in
a throttled or partial one. The merge is deterministic: r2 reproduces
`sha256:50e1be47...` byte for byte.

## Step 4: PGO image

```bash
./build-pgo.sh optimized
```

1. Refuses to build if the Rust source or `Cargo.lock` differs from
   `pgo/merged.commit`. Build tooling (`Cross.toml`, `PGO.md`, `build.sh`,
   `build-pgo.sh`, `.gitignore`) may differ. `<version>` is
   `<cargo version>.custom.<HEAD sha>`, so a tooling-only commit changes the
   image tag without needing a new profile.
2. Builds with `-Lnative=/lib/native-libs -Cprofile-use=$PWD/pgo/merged.profdata
   -Cllvm-args=-pgo-warn-missing-function`.
3. Counts `hash mismatch` warnings (a function changed since the capture)
   and fails on any; prints the count of `no profile data available`
   functions (new since the capture). Cargo hides both for crates.io
   dependencies, so they only cover Vector's own crates; the commit guard
   covers the rest.
4. Pushes `<version>-pgo-alpine-arm64` and prints the digest.

`-Cllvm-args=-pgo-warn-mismatch` does not exist; rustc stops with
"Unknown command line argument".

## Step 5: verify

From observability-dev-test, with QEMU registered
(`docker run --privileged --rm tonistiigi/binfmt --install arm64`, once per
boot):

```bash
IMG=artifactory.tomtomgroup.com/docker-dev/timberio/vector:<version>-pgo-alpine-arm64
docker run --rm --platform linux/arm64 "$IMG" --version
VECTOR="docker run --rm --platform linux/arm64 -v /tmp:/tmp -v $PWD:$PWD -w $PWD $IMG" \
  o11y-proxy/test-suite/vector-unit/vector-test.sh
```

## Step 6: pin

In both values files:

```yaml
image:
  repository: artifactory.tomtomgroup.com/docker-dev/timberio/vector
  # Profile-guided (PGO) build, arm64 only: the nodeSelector keeps Vector on
  # arm64. A Vector source change needs a new profile; see
  # docs/vector-pgo-profile-regeneration.md.
  tag: <version>-pgo-alpine-arm64
  sha: sha256:<pgo digest>
```

Never put `@sha256:` in `tag:` while `sha:` is set: chart 0.51.0 renders
both and the reference is invalid. Renovate did exactly that once (#66);
`.github/renovate.json` now sets `pinDigests: false` for this image.
`render_diff.py` must show only the image and the
`app.kubernetes.io/version` label changing. After the push, check that all 9
pods run the new digest with 0 restarts.

## Step 7: A/B run (version bumps)

For a new Vector version, confirm the gain still holds: copy the
`architecture-performance-profile-prodshape-pgo-r1` Job in
`o11y-proxy/test-suite/avalanche/architecture-performance-profile-job.yaml`
to a new name, run it with `preventUpdate: true` in
`o11y-proxy/queue/crds/kustomization.yaml`, and collect it with
`collect_performance_profile.py` (see the `o11y-proxy-capacity-run` skill).
Compare against a release-image run of the same commit. Adoption rule: at
least 10% lower combined ingress and egress CPU per sample at sustained and
Peak, no steady-state p99 regression, zero errors.

## Step 8: record

- A result YAML per capture under
  `o11y-proxy/test-suite/production-load-harness/results/`
  (`vector-pgo-profile-capture-r2.yaml` is the template: commits, image
  digests, `merged_digest`, windows, CPU, throttling, pending).
- Run rows and a dated paragraph in
  `o11y-proxy/docs/v2-poc/production-capacity-review.md`.
- A comment on the Jira ticket.

## Current profile

As of 2026-10-02:

| Item | Value |
|---|---|
| Vector commit | `5bd4ebf8b4eed91172d352abb166d0eca4d37e17` (`0.58.0.custom.5bd4ebf8b4`), toolchain 1.95; later commits on this branch change only build tooling |
| Instrumented image | `0.58.0.custom.5bd4ebf8b4-pgo-instrumented-alpine-arm64@sha256:d9e913c2d055ba7fe6f483c3c07854f74e63cee29cf05c4e01d1a968fc1f6423` |
| Capture | `vector-pgo-profile-capture-r2`, 2026-10-01 14:07 to 14:28 UTC, `pgo/raw-r2/` |
| Profile | `pgo/merged.profdata`, `sha256:50e1be472d10ca4547672f5209e54ee6728d766225c92b00a7871bccab41c4fc` |
| PGO image | `0.58.0.custom.5bd4ebf8b4-pgo-alpine-arm64@sha256:6ea9b40ed956f22d9f5c6e0917f9076a12c02f9e472bbdc1a7378bfd19a11d65` |
| Pinned in dev | observability-dev-test `03f6643` |

## Known failures

| Symptom | Cause | Fix |
|---|---|---|
| `profile-use file does not exist` during pass 2 | `cross` runs `cargo metadata` on the host, then builds in the container | `volumes = ["VECTOR_PGO_DIR"]` in `Cross.toml` |
| libstdc++ link errors | plain `RUSTFLAGS` replaced the config's `-Lnative` | target-specific variable that repeats `-Lnative=/lib/native-libs` |
| Build finishes instantly with the old binary | archive already existed | `build-pgo.sh` deletes it first |
| Packaging `cp` fails | `CARGO_TARGET_DIR` points at a sandbox cache | `build-pgo.sh` unsets it |
| `exec format error` running the arm64 image | QEMU binfmt lost after a reboot | `docker run --privileged --rm tonistiigi/binfmt --install arm64` |
| Truncated `.profraw` copies | `kubectl cp` | `build-pgo.sh fetch` (cat plus sha256) |
| Profile weighted toward backlog | capture rate too high for the instrumented binary | stay at 10% of sustained |
| Every HelmRelease fails after a Renovate PR | `@sha256` in `tag:` plus `sha:` | keep the digest only in `sha:` |
