# GoFTPd - Modular FTP Daemon

Lightweight FTP daemon 

## SITE Commands

- `SITE HELP` - Show commands
- `SITE ADDUSER username password ip` - Add user
- `SITE DELUSER username` - Delete user
- `SITE CHANGE username field value` - Modify user
- `SITE CHPASS username password` - Change password
- `SITE NUKE dir multiplier reason` - Nuke directory
- `SITE UNNUKE dir` - Unnuke
- `SITE TAGLINE tag` - Change tagline
- `SITE STAT` - Your stats
- `SITE USER [user]` - User info
- `SITE TIME` - Server time

## Features

✅ glftpd-compatible users/groups
✅ Password hashing (bcrypt)
✅ TLS/SSL support
✅ File transfers with ratio/credits
✅ SITE commands
✅ Plugin system

## Flags (glftpd-style)

- 1 = Admin/SiteOp
- 2 = Upload
- 3 = Download
- 4 = Nuke
- 5 = Unnuke
- 6 = Delete
- 7 = MKD
- 8 = Rename
- A-Z = Custom


To install: 
```
git clone and run build.sh
chmod +x build.sh
./build.sh
```

ps. its far from finished!

Master: 
```
./goftpd
2026/04/15 23:00:26 [VFS] Loaded 45 entries from userdata/vfs.dat
2026/04/15 23:00:26 [SlaveManager] Listening for slaves on 0.0.0.0:1099
2026/04/15 23:00:26 [MASTER] SlaveManager listening on port 1099, waiting for slaves...
2026/04/15 23:00:26 [PLUGINS] Initializing plugin system...
2026/04/15 23:00:26 [PLUGIN-MANAGER] Registered plugin: imdb
2026/04/15 23:00:26 [PLUGINS] Registered imdb plugin
2026/04/15 23:00:26 [PLUGIN-MANAGER] Registered plugin: tvmaze
2026/04/15 23:00:26 [PLUGINS] Registered tvmaze plugin
2026/04/15 23:00:26 [PLUGIN-MANAGER] Initialized 2 plugins
2026/04/15 23:00:26 [PLUGINS] All plugins initialized
2026/04/15 23:00:26 [DUPE] Enabled and initialized at ./logs/xdupe.db
2026/04/15 23:00:26 GoFTPd online at :21212 [Mode=master] [Plugins=2]
2026/04/15 23:00:32 [SlaveManager] Accepted connection from 127.0.0.1:37256
2026/04/15 23:00:32 [SlaveManager] Slave 'SLAVE1' connected from 127.0.0.1:37256
2026/04/15 23:00:32 [SlaveManager] Slave SLAVE1 disk: 67802MB free / 7560161MB total
2026/04/15 23:00:32 [SlaveManager] Starting remerge for slave SLAVE1 (instant online)
2026/04/15 23:00:32 [SlaveManager] Slave SLAVE1 is now AVAILABLE (remerge running in background)
2026/04/15 23:00:32 [SlaveManager] Remerge from SLAVE1: dir=/ files=1
2026/04/15 23:00:32 [SlaveManager] Remerge from SLAVE1: dir=/TV-1080P files=1
2026/04/15 23:00:32 [SlaveManager] Remerge from SLAVE1: dir=/TV-1080P/90.Day.The.Single.Life.Between.the.Sheets.S05E06.1080p.WEB.h264-CBFM files=16
2026/04/15 23:00:32 [SlaveManager] Remerge from SLAVE1: dir=/TV-1080P/90.Day.The.Single.Life.Between.the.Sheets.S05E06.1080p.WEB.h264-CBFM/Sample files=1
2026/04/15 23:00:32 [SlaveManager] Remerge from SLAVE1: dir=/TV-1080P/90.Day.The.Single.Life.Between.the.Sheets.S05E06.1080p.WEB.h264-CBFM files=1
2026/04/15 23:00:32 [SlaveManager] Remerge from SLAVE1: dir=/TV-1080P files=1
2026/04/15 23:00:32 [SlaveManager] Remerge from SLAVE1: dir=/TV-1080P/The.Dog.House.S09E03.1080p.WEB.H264-SKYFiRE files=1
2026/04/15 23:00:32 [SlaveManager] Remerge from SLAVE1: dir=/TV-1080P/The.Dog.House.S09E03.1080p.WEB.H264-SKYFiRE/Sample files=1
2026/04/15 23:00:32 [SlaveManager] Remerge from SLAVE1: dir=/TV-1080P/The.Dog.House.S09E03.1080p.WEB.H264-SKYFiRE files=16
2026/04/15 23:00:32 [SlaveManager] Remerge from SLAVE1: dir=/TV-1080P files=1
2026/04/15 23:00:32 [SlaveManager] Remerge from SLAVE1: dir=/TV-1080P/Tournament.of.Champions.S07E07.1080p.WEB.h264-CBFM files=1
2026/04/15 23:00:32 [SlaveManager] Remerge complete for slave SLAVE1
2026/04/15 23:00:32 [SlaveManager] Ghost files purged for SLAVE1
2026/04/15 23:00:32 [VFS] Saved 45 entries to userdata/vfs.dat
2026/04/15 23:00:36 [VFS] Saved 45 entries to userdata/vfs.dat


Slave:

 ./goftpd
2026/04/15 23:00:32 [SLAVE] Name 'slave', connecting to master
2026/04/15 23:00:32 [SLAVE] Name=SLAVE1 Master=127.0.0.1:1099 Roots=[/slave1/site]
2026/04/15 23:00:32 [Slave] SLAVE1 connecting to master at 127.0.0.1:1099
2026/04/15 23:00:32 [Slave] Connected to master
2026/04/15 23:00:32 [Slave] Registered as 'SLAVE1', entering command loop
2026/04/15 23:00:32 [Slave] Received command: AsyncCommand[index=00, name=remerge, args=[/ false 0 1776286832424 false]]
2026/04/15 23:00:32 [Slave] Starting remerge from / across 1 roots
2026/04/15 23:00:32 [Slave] Remerge: scanning root /slave1/site
2026/04/15 23:00:32 [Slave] Remerge root /slave1/site done: sent 11 directories
2026/04/15 23:00:32 [Slave] Remerge complete: 35 files, 6 dirs across 11 sent directories
2026/04/15 23:01:02 [Slave] Received command: AsyncCommand[index=01, name=ping, args=[]]

```


