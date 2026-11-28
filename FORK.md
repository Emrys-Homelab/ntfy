# Homelab fork of ntfy

This is the homelab cluster's fork of [binwiederhier/ntfy](https://github.com/binwiederhier/ntfy),
published in full so that a clone reproduces the image running at `notify.emrys.dev`.

| | |
| --- | --- |
| Upstream | [`binwiederhier/ntfy`](https://github.com/binwiederhier/ntfy) |
| Base | tag `v2.38.3`, commit `5dc000a53e595395fc7014cb561f00af82f4422e` |
| Fork branch | `homelab` — 3 commits |
| Patches | [`patches/`](patches/) — one file per commit, identical patch-ids |
| Series of record | [`MyrddinE/homelab`](https://github.com/MyrddinE/homelab), `patches/notify/` |
| Image | `registry.emrys.dev/homelab/ntfy:v2.38.3-homelab.5` (pinned by digest) |
| Pinned in | `MyrddinE/backup`, `gitops/apps/notify/deployment.yaml` |

## Why

Stock ntfy v2.38.3 already carries a sequence ID and client-side update, `message_clear` and
`message_delete`, but the server keeps that state only in an expiring cache: it cannot say whether
a message was read, and there is no endpoint to update or retract a delivered message. This fork
adds a **durable message registry** keyed by `(topic, sequence_id)` and the endpoints that use it,
plus the `reply` action type used by the Android client and `notify-bridge`:

- `PATCH`/`PUT`, `DELETE` and `GET` under `/v1/messages/<topic>/<identity>`, and
  `POST /v1/messages/<topic>/<identity>/ack`;
- `reply` as a first-class action: a button that takes typed input and POSTs it back to the
  publisher's URL.

| Patch | Change |
| --- | --- |
| `0001-Add-durable-message-identity-and-ack-API.patch` | the registry, update/retract, read/ack and `GET` by identity |
| `0002-Accept-the-reply-action-on-publish.patch` | accept the `reply` action type and carry it through untouched |
| `0003-Delete-removes-the-message-from-cache-and-clients.patch` | DELETE removes a read message from the cache and publishes `message_delete` |

## Branches

| Branch | What it is |
| --- | --- |
| `main` | upstream `v2.38.3` plus **one documentation commit** (this file, `patches/`, and the fork README). No code changes. |
| `homelab` | the fork: the three commits the patch series carries, on top of `main`. `git log main..homelab` is exactly `patches/*.patch`. |

`main` is the **fork point**, not a live mirror of upstream `main`, which has moved on. Fast-forward
it from upstream when the fork is rebased; `homelab` is then rebased onto it.

## Reproduce

```sh
git clone https://github.com/Emrys-Homelab/ntfy.git
cd ntfy && git checkout main
git am patches/*.patch        # the tree is then identical to branch `homelab`
```

## Build and where it is pinned

`v2.38.3-homelab.5` was built from branch `homelab` and is the image the deployment pins. The
in-cluster builder cannot carry a Go tree — its build context is a 1 MiB ConfigMap and the binary
alone is ~75 MB — so the binary is built in the `ntfy` dev environment (`CGO_ENABLED=1`, tags
`sqlite_omit_load_extension,osusergo,netgo`, statically linked) and the image assembled onto the
stock `binwiederhier/ntfy:v2.38.3` with `crane`, as an **OCI layout** (zot rejects the Docker
manifest that `crane append` produces).

The digest is pinned in `MyrddinE/backup`, `gitops/apps/notify/deployment.yaml`; `crane digest`
prints it for the tag. `homelab.5` follows `homelab.4` (base `v2.37.2`, commit `442eaf11`).
