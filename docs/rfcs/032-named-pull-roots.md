# RFC 032: Named pull roots — pull a file by {root, relative path}

**Status**: Proposed, implemented in the same change (core, NAPI, TypeScript)
**Author**: James Yong + Claude
**Date**: 2026-11-06
**Depends on**: RFC 014 (file transfer in core), RFC 016 (request/reply), the pull-root allowlist (0.4.8, #133)
**Origin**: VibeField's Drive (DR-3: objects move between a user's desktops by `file_transfer` pulls)
**Numbering**: 029 is reserved (iOS on truffle-core, named in RFC 028); 030 is T-1 (response headers per
serve) and 031 is T-2 (a service name per serve), both from the same VibeField record.

---

## 1. Problem

A pull names the file by the **holder's absolute path**: `pull_file(peer, remote_path, local_path)` sends
`PULL_REQUEST {path}`, and the holder canonicalizes `path` and serves it only if it lies inside one of its
`add_pull_root` roots (`file_transfer/receiver.rs`, `authorize_pull_path`).

So a requester must know where the holder keeps its files. Every holder has to publish its absolute layout to
its peers, and that layout differs per device: a user's home, a per-user data root, a volume. VibeField's
Drive is the first consumer that hits this. Each desktop holds content-addressed objects under its own
`users/<id>/drive/objects/`, and without this RFC it would have to project that absolute path to every peer
through a `SyncedStore` slice. That is a filesystem layout leaving the device, for no reason except the wire
shape.

## 2. Decision

A holder registers a directory **under a name**; a requester pulls **by name and relative path**.

Core (`FileTransfer`):

| Method | |
|---|---|
| `add_named_pull_root(name, root) -> io::Result<()>` | register (or replace) `name` → canonical `root` |
| `named_pull_roots() -> Vec<NamedPullRoot>` | list, sorted by name |
| `remove_named_pull_root(name) -> bool` | unregister one |
| `pull_named_file(peer, root, path, local_path)` | pull `path` below the peer's root `root` |
| `clear_pull_roots()` | now clears named roots too (deny-by-default for both) |

New public types: `NamedPath {root, path}` (the wire target), `NamedPullRoot {name, path}` (a listing row),
`PULL_DENIED_REASON` and `NAMED_PULL_DENIED_REASON` (the two REJECT reasons), and
`TransferError::Unsupported(String)`.

NAPI (`NapiFileTransfer`): `pullNamedFile(peerId, root, path, localPath)`, `addNamedPullRoot(name, root)`,
`namedPullRoots(): NapiNamedPullRoot[]`, `removeNamedPullRoot(name): boolean`. `@vibecook/truffle`'s
`mesh.fileTransfer().pullNamedFile(to: PeerLike, root, path, localPath)` routes a Peer handle by its ref, as
`sendFile` and `pullFile` do.

## 3. Semantics (normative)

### 3.1 Names

A root name matches `[a-z0-9][a-z0-9._-]{0,63}`. Names are lowercase by grammar, so a lookup is exact and
never folds case: `Objects` cannot be registered, and a pull naming it never reaches `objects`.

### 3.2 Relative paths

A named pull's `path` is `/`-separated and normalized. It is refused when it:
- is empty or longer than 4096 bytes;
- is absolute (starts with `/`);
- has an empty, `.` or `..` segment (so no `//`, no trailing `/`);
- contains NUL, `\` or `:`.

`\` and `:` are refused so a path names the same file on every holder platform: on Windows `\` separates and
`:` starts a drive (`C:x`) or an alternate data stream (`f.txt:s`).

So `path` is a wire string, never an OS path: a requester joins its segments with `/` on every platform. A
relative path built as a Windows `PathBuf` (`ab\ab01…`) is refused on the requester before anything is sent.

The requester checks both grammars before it sends anything (`TransferError::Io` with `InvalidInput`). The
holder checks again and never trusts the requester's check.

### 3.3 Authorization on the holder

1. Look the root up by exact name. Unknown → refused.
2. Check the relative path (§3.2).
3. Canonicalize `root.join(path)`; this resolves every symlink. Refused unless the result still lies inside
   the root (`Path::starts_with`, component-wise, so a sibling `objects-evil` never matches `objects`).
4. Refused unless it is a regular file no larger than the max transfer size.

A symlink whose target stays inside the root serves that target; one that leaves the root is refused, whether
it is the file or a directory on the way.

**The two tables are disjoint.** A named root answers named pulls only; an absolute-path pull never reaches it,
even when the requester names the file's exact absolute path. A named pull never reaches an `add_pull_root`
root. Registering a name therefore never widens the absolute-path surface.

A named pull that also carries a non-empty absolute `path` is refused as ambiguous.

### 3.4 Refusals

Every refused named pull gets one REJECT reason, `named pull denied by peer` (`NAMED_PULL_DENIED_REASON`),
whatever the cause, as absolute-path pulls get `pull denied by peer` (`PULL_DENIED_REASON`). The requester
learns nothing about what exists on the holder. The holder logs the actual cause locally. A busy holder still
answers `receiver busy`.

### 3.5 Serving

As for an absolute-path pull: OFFER, ACCEPT, raw TCP, SHA-256. The OFFER's `file_name` is the last segment
of the relative path. As with absolute-path roots, a holder answers pulls only while its file-transfer
receiver runs (`offer_channel`, `auto_accept` or `auto_reject` starts it).

One thing differs: **the OFFER that answers a named pull carries the PULL_REQUEST's own `token`**, where an
absolute-path pull's OFFER carries a fresh one. Every pull waiting on a node sees every `ft` message, so with
several pulls in flight to one holder each must know which answer is its own. The requester takes:

- a REJECT only when it carries the pull's `token`, on every pull. Every holder that refuses a pull (0.4.8 on)
  echoes the token, so this costs no compatibility;
- on a named pull, an OFFER only when it carries the pull's `token`. No holder that predates this RFC offers a
  file for a named pull (§5), so this costs no compatibility either;
- on an absolute-path pull, the peer's OFFER whatever its token, as before. Its JSON and every earlier holder's
  OFFER are unchanged, so two absolute-path pulls in flight to one holder can still take each other's OFFER.

## 4. Wire

`PULL_REQUEST` gains an optional `named` object; a named pull sends an **empty** `path`:

```json
{"type":"pull_request","path":"","requester_id":"01J…","token":"…",
 "named":{"root":"drive-objects","path":"ab/ab01….bin"}}
```

`named` is `#[serde(default, skip_serializing_if = "Option::is_none")]`, so an absolute-path pull's JSON is
byte-identical to before, and a current holder reads an earlier requester's pull exactly as before.

The holder answers a named pull with an OFFER whose `token` is the PULL_REQUEST's `token` (§3.5); the OFFER's
shape is unchanged.

## 5. Mixed versions

The empty `path` is the compatibility mechanism. A holder that predates this RFC ignores `named` (serde ignores
unknown fields) and sees only `path: ""`, which no holder can resolve to a file: `canonicalize("")` fails
(`realpath("")` is `ENOENT`; Windows refuses an empty path), even when a pull root covers the holder's working
directory.

| Requester → holder | Outcome |
|---|---|
| current → current | served, or `Rejected("named pull denied by peer")` |
| current → 0.4.8 … 0.7.26 | the holder refuses with `pull denied by peer`; the requester reports `TransferError::Unsupported` |
| current → before 0.4.8 | the holder's read of `""` fails and it sends no REJECT; the requester times out (`TransferError::Timeout`, 30 s) |
| current → a holder whose receiver is not running | no answer; `Timeout`, as for any pull |
| earlier → current | absolute-path pulls only, served exactly as before |

**A named pull never yields a wrong file**: an earlier holder can only refuse it or stay silent, and a current
holder's OFFER names the pull it answers (§3.5), so a named pull never takes another pull's OFFER.

The requester tells an earlier holder from a refusal by the reason alone, on the REJECT that carries this
pull's `token` (§3.5): on a named pull, `pull denied by peer` can only come from a holder that ignored `named`,
because a current holder refuses a named pull only with `named pull denied by peer` or `receiver busy`. Another
pull's REJECT, whatever its reason, is not this pull's answer. No hello or capability exchange is needed, and
none was added.

## 6. Security

- **No existence oracle**: one reason per kind of pull (§3.4).
- **Symlinks** are resolved before the containment check (§3.3).
- **Disjoint tables** (§3.3): sharing by name widens nothing else.
- **Root-name probing**: an unknown name and a missing file get the same answer, so a peer cannot enumerate
  names.
- **Time of check and time of use**: as for absolute-path pulls, the canonical path is opened after the check;
  a same-user process that swaps a component for a symlink in between is outside this RFC's threat model, as it
  is outside the allowlist's.

## 7. Compatibility of the Rust API

- `FtMessage::PullRequest` gains a field. Code outside the crate that builds the variant with a struct
  literal adds `named: None`; patterns add `..` or bind it.
- `TransferError` gains `Unsupported`, and becomes `#[non_exhaustive]` in the same change. A `match` outside
  the crate adds a wildcard arm once, as for `NetworkError::EgressError` (RFC 026), and a later variant no
  longer breaks it.

## 8. Not in scope

- **Starting pull serving without the offer channel.** A holder that only serves pulls must still call
  `offer_channel`, `auto_accept` or `auto_reject`, and the offer channel is single per node, so a pull-only
  holder that uses `auto_reject` collides with a later push consumer. A `serve_pulls()` that starts the receiver
  without claiming the channel is a separate change.
- **The Tauri plugin and the CLI.** They expose absolute-path pulls today; named pulls follow when a consumer
  needs them there.
- **Swift and Android.** Neither plane speaks `ft` itself.

## 9. Tests

- `file_transfer/named.rs`: both grammars (accepted and refused sets, case, the wire form against a Windows
  path), authorization (unknown root, exact names, `..` and absolute paths re-checked, a prefix sibling reached
  through a symlink so only the component-wise check refuses it, missing, directory, oversize, a symlinked file
  and a symlinked directory that leave the root, a symlink that stays inside), and the requester's reading of a
  REJECT (`Unsupported` only for the legacy reason on a named pull).
- `file_transfer/pull_tests.rs`, an in-process pair (two nodes over loopback, no tailnet): a named pull lands
  byte for byte; an unknown root, a missing file and a symlink out of the root each get the named reason; the
  named file's absolute path gets the absolute-path reason; two concurrent named pulls each land their own
  file; a refusal answers only the pull it names. Unit rows feed the requester single messages: a REJECT or a
  named pull's OFFER with another pull's token is not this pull's answer, an absolute-path pull takes its
  peer's OFFER whatever its token, and nothing from another peer answers.
- `file_transfer/types.rs`: the absolute-path JSON is unchanged; an earlier requester's JSON parses with no
  `named`; a named pull round-trips; the 0.4.8–0.7.26 parser sees only the empty path.
- `file_transfer/receiver.rs`: the legacy authorization refuses the empty path even with the working directory
  shared; `handle_pull_request` authorizes a named file and refuses every rule break before any I/O; the two
  tables never answer for each other; registration validates, lists, replaces, removes and clears; the
  requester refuses a bad name or path before any network.
- `tests/integration_file_transfer.rs` (real tailnet pair, `TRUFFLE_TEST_AUTHKEY`): a named pull lands byte for
  byte, an absolute-path pull still works, an unknown root is a named denial, the named file's absolute path is
  refused, a symlink out of the root is refused.
- `packages/core/test/file-transfer.test.cjs`: `pullNamedFile` routes a Peer by its ref and passes root and
  path through.
