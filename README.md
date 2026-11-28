# ntfy — homelab fork

This is the emrys.dev homelab's fork of [ntfy](https://github.com/binwiederhier/ntfy), a
self-hosted push-notification server. It adds a durable record of delivered messages (update,
retract, read/ack) and a `reply` action, on top of upstream **v2.38.3**.

**Upstream project: https://github.com/binwiederhier/ntfy**

Upstream is the source of truth for installing, configuring and using ntfy; this README describes
only what the fork changes. The unedited upstream README is preserved at
[README.upstream.md](https://github.com/Emrys-Homelab/ntfy/blob/main/README.upstream.md).

## Base

Upstream tag `v2.38.3`, commit `5dc000a53e595395fc7014cb561f00af82f4422e` (2026-11-27). The fork
was rebased from `v2.37.2`, so it also carries the three releases between the old base and this
one: `v2.38.0` (cluster-shared rate limits, Go 1.27.4, WebSocket drain fix), `v2.38.1` (message
templating hardening, S3 upload buffering, web push delivery timeouts) and `v2.38.2` (all
`X-Forwarded-For` header lines are read, so a client behind a proxy can no longer spoof its IP and
bypass the per-IP rate and login limits; SMTP library fix; reservation fixes). `v2.38.3` adds the
web push endpoint SSRF fix and `experimental-firebase-limit`.

## What this fork changes

- **0001 — durable message identity, update, retract and read/ack.** A SQLite registry keyed by
  `(topic, sequence_id)` stores the last payload, a revision counter, the read/ack timestamps and
  the retraction state, and serves `PATCH`/`PUT`, `DELETE` and `GET` under
  `/v1/messages/<topic>/<identity>` plus `POST …/ack`. The state outlives the 48-hour message cache
  and a restart.
- **0002 — the `reply` action.** A first-class action type that renders a button taking typed
  input and POSTs it to the publisher's URL. The server validates and delivers it; the endpoint it
  points at is your own.
- **0003 — DELETE means delete.** DELETE removes every cached revision of the message and
  publishes `message_delete` for a read message too, instead of leaving a log-only tombstone; the
  registry keeps the retraction state so a replay emits nothing.

## Branches

| Branch | What it is |
| --- | --- |
| `main` | upstream tag `v2.38.3` plus one documentation commit: this README, `README.upstream.md`, `FORK.md` and `patches/`. No code changes. |
| `homelab` | the fork: `main` plus the three patches, and the same documentation. This is the branch to build. |

Base: upstream tag `v2.38.3`, commit `5dc000a53e595395fc7014cb561f00af82f4422e`.

## Build and deploy

The image is the stock `binwiederhier/ntfy:v2.38.3` image with one layer replacing
`/usr/bin/ntfy`. The binary is built from `homelab` with `CGO_ENABLED=1`, tags
`sqlite_omit_load_extension,osusergo,netgo`, statically linked, with upstream's Vite web app and
mkdocs assets embedded. It is assembled with `crane` as an **OCI layout** — zot rejects the Docker
manifest `crane append` produces — and pushed.

`~/homelab/scripts/build-image.sh` is deliberately not the path here: it carries the build context
in a 1 MiB ConfigMap, and the Go tree plus the ~75 MB binary cannot fit.

The deployment pins the digest, not the tag:

```
registry.emrys.dev/homelab/ntfy:v2.38.3-homelab.5@sha256:…
```

That reference is in `~/backup/gitops/apps/notify/deployment.yaml`; the registry settings
(`message-registry-file: /var/cache/notify/registry.db`, `message-registry-duration: 720h`) are in
`~/backup/gitops/apps/notify/configmap.yaml`, on the `notify-cache` PVC. A rebuild means a new
digest and a commit in `~/backup`.

## Patches

The series is in [`patches/`](https://github.com/Emrys-Homelab/ntfy/tree/main/patches), one file
per commit. Applying it to `main` reproduces the code on `homelab`:

```sh
git checkout main
git am patches/*.patch
```

The series of record, with the endpoint contract, the delete/read behaviour and the re-verification
notes, is [`MyrddinE/homelab`](https://github.com/MyrddinE/homelab), `patches/notify/`.
