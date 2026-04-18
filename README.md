# GoFTPd

A distributed FTP daemon written in Go, drftpd-inspired, glftpd-compatible. Master/slave architecture with VFS-based zipscript, live race stats, TLS 1.3, CRC32 verification, and an IRC sitebot for announces.

## Architecture

Master handles FTP protocol, auth, VFS, zipscript, race DB, CRC32 checks. Slaves handle disk I/O.

**Passthrough mode (recommended)** — client connects directly to the slave:
```
FTP client <--TLS--> slave (disk I/O)
    |                  |
    +-- control -- master (VFS, auth, CRC, race stats, sitebot events)
```

**Proxy mode (fallback)** — master relays all data:
```
FTP client <--TLS--> master <--TCP--> slave (disk I/O)
```

|                       | Passthrough                       | Proxy                              |
|-----------------------|-----------------------------------|------------------------------------|
| Bandwidth cost        | 1x (client↔slave)                 | 2x (client→master + master→slave)  |
| Speed limit           | Slave's line speed                | Master's line speed                |
| CRC32                 | Slave calculates, master verifies | Master calculates on the fly       |
| Requires              | Slave reachable by client         | Only master needs public IP        |
| FXP                   | Works                             | Works                              |
| Config                | `passthrough: true` (default)     | `passthrough: false`               |

You can run a slave on the same box as the master to serve local storage.

## Quick Start

```bash
# Build
./build.sh

# Generate TLS certs (ECDSA P-384, TLS 1.3 AES-256-GCM)
./generate_certs.sh

# Configure master
cp etc/config-master.yml etc/config.yml
# edit: set public_ip, listen_port, pasv range, storage_path, slaves:

# Start master
./goftpd

# Start slave (same or different box)
cp etc/config-slave.yml etc/config.yml
# edit: set slave.name, master_host, roots
./goftpd
```

Default login: `goftpd` / `goftpd` (siteop). Change it immediately with `SITE CHPASS`.

## Features

### Transfer & Security
- TLS 1.3, ECDSA P-384 certs (`TLS_AES_256_GCM_SHA384`)
- AUTH TLS, PBSZ, PROT, SSCN, CPSV (FXP)
- PRET, PASV, PORT modes
- XDUPE duplicate detection
- Thread-safe gob streams between master and slaves

### Zipscript (master-side, VFS-based)
- CRC32 verification on upload via `io.TeeReader` — zero extra pass over the data
- Hardware-accelerated CRC32 (SSE4.2/CLMUL on x86, ARMv8 CRC on ARM)
- 0-byte file rejection
- CRC mismatch → file deleted, client gets `550`
- Files not in the SFV pass through (NFO, sample, tags, proofs)
- Virtual LIST entries: progress bars, `[COMPLETE]` tags, `-MISSING` files
- Live race stats on `CWD` into a release (CP437 box-drawing, ASCII logo)
- Per-user and per-group stats tracked in SQLite race DB
- No disk writes during uploads — all state lives in the VFS

### Slave Affinity & Load Balancing
Route sections to specific slaves, balance load across multiple slaves, weight slaves by capacity. Configured on the master:

```yaml
slaves:
  - name: "SLAVE1"
    sections: ["TV-1080P", "TV-720P"]
    weight: 2
  - name: "SLAVE2"
    sections: ["MP3", "FLAC"]
    weight: 1
  - name: "SLAVE3"
    # no sections = overflow for anything unmatched
    weight: 1
```

Selection order: section match → lowest `activeTransfers / weight` → most free disk space. If no policy matches, the master falls back to all available slaves (fail-open).

### User Management
- glftpd-compatible user and group files
- bcrypt and Apache MD5 (`apr1`) password hashing — unknown `$`-formats rejected fail-closed
- Per-user flags, groups, IPs, credits, ratios
- Path-based ACL engine

### Plugins
- **IMDb** — movie info lookup
- **TVMaze** — TV show info lookup
- **sitebot** — IRC bot with FiSH Blowfish (CBC), pzs-ng-style themes, real-time race/stats announces

## FTP Commands

`FEAT` `OPTS` `USER` `PASS` `SYST` `TYPE` `REST` `PWD` `CWD` `CDUP` `MKD` `RMD` `SIZE` `MDTM` `DELE` `RNFR` `RNTO` `PASV` `PORT` `LIST` `MLSD` `STOR` `RETR` `ABOR` `NOOP` `PRET` `PBSZ` `PROT` `SSCN` `CPSV` `AUTH TLS` `SITE` `XDUPE`

## SITE Commands

### User Management (siteop: flag `1`)
| Command   | Usage                                | Description                          |
|-----------|--------------------------------------|--------------------------------------|
| `ADDUSER` | `<user> <pass> [ident@ip ...]`       | Create user (fails if exists)        |
| `DELUSER` | `<user>`                             | Delete user                          |
| `CHPASS`  | `<user> <newpass>`                   | Change password (bcrypt)             |
| `ADDIP`   | `<user> <ident@ip> [...]`            | Add IP(s), auto-prefixes `*@`        |
| `DELIP`   | `<user> <ident@ip> [...]`            | Remove IP(s)                         |
| `FLAGS`   | `<user> <+\|-\|=><flags>`            | Modify flags                         |
| `CHGRP`   | `<user> <group> [...]`               | Toggle group membership              |
| `CHPGRP`  | `<user> <group>`                     | Set primary group                    |
| `GADMIN`  | `<user> <group>`                     | Grant group admin                    |

### Group Management
| Command  | Usage             | Description     |
|----------|-------------------|-----------------|
| `GRPADD` | `<name> [desc]`   | Create group    |
| `GRPDEL` | `<name>`          | Delete group    |
| `GRP`    |                   | List groups     |

### Release Management
| Command  | Usage                           | Description    |
|----------|---------------------------------|----------------|
| `NUKE`   | `<dir> <mult> <reason>`         | Nuke release   |
| `UNNUKE` | `<dir>`                         | Undo nuke      |

### Informational
`HELP` `RULES` `WHO` `INVITE` `CHMOD` `XDUPE`

## User Flags

| Flag | Role                   |
|------|------------------------|
| `1`  | Siteop                 |
| `2`  | Group admin            |
| `3`  | Regular user           |
| `4`  | Exempt from stats      |
| `5`  | Exempt from credits    |
| `6`  | Can kick users         |
| `7`  | See hidden dirs        |

## Sitebot (IRC Announces)

The sitebot reads events from a FIFO and posts to IRC. Non-blocking FIFO writer — the FTP daemon never stalls when IRC is throttled or the bot is slow.

### Event Types
Individual events published in real time: `NEWDIR`, `SFV_RAR`, `UPDATE_RAR`, `RACE_RAR`, `NEWLEADER`, `HALFWAY`, `COMPLETE`, `STATS_HOF`, `STATS_SPEEDS`, `STATS_USER`, `STATS_END`, `TVINFO`, `NUKE`, `UNNUKE`.

### Theme
pzs-ng-style `.theme` file at `sitebot/etc/templates/pzsng.theme`. Supports `%b{}` bold, `%cNN{}` mIRC color, `%u{}` underline, `%{varname}` variable expansion. Ships with a light-background-friendly default (dark colors only — no grey/yellow).

### Channel Routing
Sections route to channels via the sitebot config:
```yaml
sections:
  - name: "TV-1080P"
    channels: ["#goftpd"]
    paths: ["/TV-1080P/*"]
  - name: "MP3"
    channels: ["#goftpd-spam"]
    paths: ["/MP3/*"]
```
`type_routes` (e.g. routing all NUKEs to a dedicated channel) overrides section routing.

### Encryption
Per-channel Blowfish keys via FiSH CBC (`cbc:` prefix). Use `plain:` for ECB (legacy).

## Configuration Files

| File                            | Purpose                                |
|---------------------------------|----------------------------------------|
| `etc/config.yml`                | Active master/slave config             |
| `etc/config-master.yml`         | Master example (with slave policies)   |
| `etc/config-slave.yml`          | Slave example                          |
| `etc/passwd`                    | Password hashes                        |
| `etc/users/`                    | User files (glftpd format)             |
| `etc/groups/`                   | Group files                            |
| `etc/msgs/`                     | Message templates                      |
| `sitebot/etc/config.yml`        | Sitebot config                         |
| `sitebot/etc/templates/*.theme` | Announce themes                        |

## Project Structure

```
cmd/goftpd/          Entry point
internal/
  acl/               Path-based ACL engine
  config/            Master/slave YAML parsing
  core/              FTP protocol, SITE commands, race renderer, auth, events
  dupe/              XDUPE handling
  master/            Bridge, VFS, slave manager, remote slave, race DB
  plugin/            Plugin interface
  protocol/          Master↔slave gob wire protocol
  slave/             Slave daemon, transfer handler
  user/              User loading/saving (glftpd format)
plugins/
  imdb/              IMDb lookup
  tvmaze/            TVMaze lookup
sitebot/
  cmd/gositebot/     Sitebot entry point
  internal/
    bot/             Bot coordinator, channel routing
    event/           Event type definitions
    irc/             IRC client + FiSH/Blowfish
    plugin/          Announce, TVMaze, IMDb
    template/        Theme parser/renderer
  etc/templates/     Announce themes
```

## Changelog

### v1.0.2
- **Sitebot** with real-time per-event IRC announces (NEW, RACE, NEWLEADER, HALFWAY, COMPLETE, STATS, TV-INFO, NUKE, UNNUKE)
- Non-blocking FIFO writer — FTP stays fast regardless of IRC throttling
- FiSH Blowfish CBC encryption (random IV, zero-padding)
- pzs-ng-style theme engine with `%b{}` / `%cNN{}` / `%u{}` / `%{var}` syntax
- Light-background theme shipped by default
- Per-user peak speed tracking for slowest/fastest stats
- Section-to-slave affinity with weighted load balancing
- Active-transfer counter on each slave for load-balancer scoring
- Race DB refactored (fixed nested-cursor SQLite issue under concurrent load)

### v1.0.1b
- **Passthrough transfer mode** — direct client→slave transfers, no master bandwidth bottleneck
- PRET stores upcoming transfer type for slave selection at PASV time
- Race stats rendered in code with CP437 box-drawing and ASCII logo
- SITE `FLAGS` (`+`/`-`/`=`), `CHGRP` toggle, `ADDUSER` / `DELUSER` / `CHPASS` / `ADDIP` / `DELIP`
- Password verification: unknown `$`-formats rejected
- Apache MD5 (`apr1`) hash support
- No `.message` disk writes — race stats fully live from VFS
- Write mutex on master + slave gob streams (fixes concurrent upload crashes)
- CRC32 verification via `io.TeeReader` during bridge upload
- 0-byte file rejection

### v1.0.0
- Initial master/slave architecture
- VFS-based zipscript
- TLS 1.3 with ECDSA P-384

## License

MIT

## Credits

Inspired by [drftpd](https://github.com/drftpd-ng/drftpd), [glftpd](https://glftpd.io), and [pzs-ng](https://github.com/pzs-ng/pzs-ng).