package core

import (
	"crypto/tls"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"io/fs"
	"log"
	"net"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"

	"goftpd/internal/timeutil"
	"goftpd/internal/user"
	"goftpd/internal/zipscript"
)

// getMlsdPerm returns MLSD permissions string for a file
func getMlsdPerm(info os.FileInfo, isSymlink bool) string {
	perms := ""
	if isSymlink {
		perms = "flr"
	} else if info.IsDir() {
		perms = "flcdmpe"
	} else {
		perms = "flrwd"
	}
	return perms
}

// isSceneSubfolder returns true if name is a conventional subfolder that lives
// INSIDE a scene release (not a release itself). These are created repeatedly
// across every release and must not trigger dupe-check.
func isSceneSubfolder(name string) bool {
	lower := strings.ToLower(name)
	switch lower {
	case "sample", "samples", "proof", "proofs", "subs", "sub", "subtitles",
		"cover", "covers", "covers-back", "covers-front", "covers-side",
		"extras", "extra", "featurettes", "nfo":
		return true
	}
	// CD1, CD2, ..., DVD1, DVD2, DISC1, etc.
	if m, _ := regexp.MatchString(`^(cd|dvd|disc|disk)\d+$`, lower); m {
		return true
	}
	return false
}

// processCommand handles the core RFC 959 FTP and FXP commands.
func (s *Session) processCommand(cmd string, args []string, tlsConfig *tls.Config) bool {
	switch cmd {
	case "FEAT":
		fmt.Fprintf(s.Conn, "211- Extensions supported:\r\n")
		if s.Config != nil && s.Config.TLSEnabled {
			fmt.Fprintf(s.Conn, " AUTH TLS\r\n")
			fmt.Fprintf(s.Conn, " PBSZ\r\n")
			fmt.Fprintf(s.Conn, " PROT\r\n")
			fmt.Fprintf(s.Conn, " SSCN\r\n")
			fmt.Fprintf(s.Conn, " CPSV\r\n")
		}
		fmt.Fprintf(s.Conn, " SIZE\r\n")
		fmt.Fprintf(s.Conn, " MDTM\r\n")
		fmt.Fprintf(s.Conn, " MLSD\r\n")
		fmt.Fprintf(s.Conn, " MLST Type*;Size*;Modify*;Perm*;\r\n")
		fmt.Fprintf(s.Conn, " REST STREAM\r\n")
		fmt.Fprintf(s.Conn, " PRET\r\n")
		fmt.Fprintf(s.Conn, " SITE\r\n")
		fmt.Fprintf(s.Conn, " UTF8\r\n")
		fmt.Fprintf(s.Conn, "211 End\r\n")

	case "OPTS":
		if len(args) > 0 && strings.ToUpper(args[0]) == "UTF8" {
			fmt.Fprintf(s.Conn, "200 UTF8 set to on\r\n")
		} else {
			fmt.Fprintf(s.Conn, "200 OPTS accepted.\r\n")
		}

	case "PBSZ":
		if s.Config == nil || !s.Config.TLSEnabled {
			fmt.Fprintf(s.Conn, "500 TLS not configured\r\n")
			return false
		}
		if !s.IsTLS {
			fmt.Fprintf(s.Conn, "503 Security exchange needs to be completed first\r\n")
			return false
		}
		if len(args) == 0 {
			fmt.Fprintf(s.Conn, "501 Syntax error\r\n")
			return false
		}
		if strings.TrimSpace(args[0]) != "0" {
			fmt.Fprintf(s.Conn, "200 PBSZ=0\r\n")
			return false
		}
		fmt.Fprintf(s.Conn, "200 PBSZ 0 successful\r\n")

	case "PROT":
		if s.Config == nil || !s.Config.TLSEnabled {
			fmt.Fprintf(s.Conn, "500 TLS not configured\r\n")
			return false
		}
		if !s.IsTLS {
			fmt.Fprintf(s.Conn, "500 You are not on a secure channel\r\n")
			return false
		}
		if len(args) == 0 {
			s.DataTLS = false
			fmt.Fprintf(s.Conn, "200 Command OK\r\n")
			return false
		}
		if len(strings.TrimSpace(args[0])) != 1 {
			fmt.Fprintf(s.Conn, "501 Syntax error\r\n")
			return false
		}
		switch strings.ToUpper(args[0]) {
		case "P":
			s.DataTLS = true
			fmt.Fprintf(s.Conn, "200 Protection set to Private\r\n")
		case "C":
			s.DataTLS = false
			fmt.Fprintf(s.Conn, "200 Protection set to Clear\r\n")
		default:
			fmt.Fprintf(s.Conn, "501 Syntax error\r\n")
			return false
		}

	case "SSCN":
		if !s.IsTLS {
			fmt.Fprintf(s.Conn, "500 You are not on a secure channel\r\n")
			return false
		}
		if !s.DataTLS {
			fmt.Fprintf(s.Conn, "500 SSCN only works for encrypted transfers\r\n")
			return false
		}
		if len(args) > 0 {
			switch strings.ToUpper(args[0]) {
			case "ON":
				s.SSCN = true
			case "OFF":
				s.SSCN = false
			}
		}
		if s.SSCN {
			fmt.Fprintf(s.Conn, "220 SSCN:CLIENT METHOD\r\n")
		} else {
			fmt.Fprintf(s.Conn, "220 SSCN:SERVER METHOD\r\n")
		}

	case "CPSV":
		if s.Config.Debug {
			log.Printf("[CPSV] Starting passive mode setup (passthrough=%v)", s.Config.Passthrough)
		}
		s.clearActiveTransferSetup()
		if !s.IsTLS {
			fmt.Fprintf(s.Conn, "500 You are not on a secure channel\r\n")
			return false
		}
		if !s.DataTLS {
			fmt.Fprintf(s.Conn, "500 SSCN only works for encrypted transfers\r\n")
			return false
		}
		if !hasPretForPassive(s) {
			s.clearPreparedTransferState()
			fmt.Fprintf(s.Conn, "500 You need to use a client supporting PRET (PRE Transfer) to use PASV\r\n")
			return false
		}

		if s.Config.Passthrough && s.Config.Mode == "master" && s.MasterManager != nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				targetPath := s.resolvePretTargetPath(bridge)

				var slaveIP string
				var port int
				var xferIdx int32
				var slaveName string
				var err error
				if s.PretCmd == "RETR" {
					slaveIP, port, xferIdx, slaveName, err = bridge.SlaveListenForDownloadPassthrough(targetPath, s.DataTLS, true)
				} else {
					slaveIP, port, xferIdx, slaveName, err = bridge.SlaveListenForPassthrough(targetPath, s.DataTLS, true)
				}
				if err != nil {
					log.Printf("[CPSV] Passthrough slave listen failed for user %s path %s: %v", s.User.Name, targetPath, err)
					fmt.Fprintf(s.Conn, "450 No available slave for upcoming transfer.\r\n")
					return false
				}
				s.PassthruSlave = slaveName
				s.PassthruXferIdx = xferIdx
				if s.DataListen != nil {
					s.DataListen.Close()
					s.DataListen = nil
				}
				ip, err := ftpPassiveIPv4(slaveIP)
				if err != nil {
					fmt.Fprintf(s.Conn, "500 Invalid passive address for selected slave.\r\n")
					s.clearPassiveTransferSetup()
					return false
				}
				response := fmt.Sprintf("227 Entering Passive Mode (%s,%d,%d)\r\n", ip, port/256, port%256)
				if s.Config.Debug {
					log.Printf("[CPSV] Passthrough to slave %s: %s (port: %d)", slaveName, strings.TrimSpace(response), port)
				}
				fmt.Fprintf(s.Conn, response)
				return false
			}
		}

		var l net.Listener
		var port int
		var err error
		for p := s.Config.PasvMin; p <= s.Config.PasvMax; p++ {
			l, err = net.Listen("tcp", fmt.Sprintf(":%d", p))
			if err == nil {
				port = p
				break
			}
		}
		if l == nil {
			fmt.Fprintf(s.Conn, "425 Can't open passive data connection.\r\n")
			return false
		}
		s.DataListen = l
		s.PassthruSlave = nil
		s.nextDataTLSClientMode = true
		ip, err := ftpPassiveIPv4(s.Config.PublicIP)
		if err != nil {
			fmt.Fprintf(s.Conn, "500 Invalid passive address configuration.\r\n")
			if s.DataListen != nil {
				s.DataListen.Close()
				s.DataListen = nil
			}
			return false
		}
		fmt.Fprintf(s.Conn, "227 Entering Passive Mode (%s,%d,%d)\r\n", ip, port/256, port%256)
		return false

	case "USER":
		if len(args) == 0 {
			fmt.Fprintf(s.Conn, "501 Syntax error.\r\n")
			return false
		}
		requestedUser := strings.TrimSpace(args[0])
		normalizedUser := normalizeLoginUsername(requestedUser)
		if normalizedUser == "" {
			s.PendingUser = requestedUser
			s.PendingReason = "unknown_user"
			s.User = nil
			fmt.Fprintf(s.Conn, "331 Password required for %s\r\n", requestedUser)
			return false
		}
		s.PendingUser = normalizedUser
		s.PendingReason = ""
		s.User = nil
		u, err := user.LoadUser(normalizedUser, s.GroupMap)
		if err != nil {
			if _, statErr := os.Stat(deletedUserPath(normalizedUser)); statErr == nil {
				s.PendingReason = "user_deleted"
			} else {
				s.PendingReason = "unknown_user"
			}
			if s.Config.Debug {
				log.Printf("[AUTH] Failed to load user '%s' (requested as '%s'): %v", normalizedUser, requestedUser, err)
			}
		} else {
			s.User = u
		}
		fmt.Fprintf(s.Conn, "331 Password required for %s\r\n", normalizedUser)

	case "PASS":
		if s.User != nil {
			remoteIP, _, _ := net.SplitHostPort(s.Conn.RemoteAddr().String())

			if s.Config.IPRestrictions != nil {
				if restrictedIPs, ok := s.Config.IPRestrictions[s.User.Name]; ok && len(restrictedIPs) > 0 {
					allowed := false
					for _, allowedIP := range restrictedIPs {
						if remoteIP == allowedIP || allowedIP == "*" {
							allowed = true
							break
						}
					}
					if !allowed {
						if s.Config.Debug {
							log.Printf("[PASS] User %s login rejected: IP %s not in whitelist", s.User.Name, remoteIP)
						}
						s.emitLoginFailure(s.User.Name, remoteIP, "ip_restricted")
						fmt.Fprintf(s.Conn, "530 Login not allowed from this IP.\r\n")
						return false
					}
				}
			}

			if s.User.IsExpired() {
				s.emitLoginFailure(s.User.Name, remoteIP, "account_expired")
				fmt.Fprintf(s.Conn, "530 Account expired.\r\n")
				return false
			}
			if s.User.IsDisabled() {
				s.emitLoginFailure(s.User.Name, remoteIP, "account_disabled")
				fmt.Fprintf(s.Conn, "530 Account disabled.\r\n")
				return false
			}
			if ban, ok := FindUserBan(s.User.Name); ok {
				s.emitLoginFailure(s.User.Name, remoteIP, "account_banned")
				if strings.TrimSpace(ban.Reason) != "" {
					fmt.Fprintf(s.Conn, "530 Account banned: %s.\r\n", ban.Reason)
				} else {
					fmt.Fprintf(s.Conn, "530 Account banned.\r\n")
				}
				return false
			}

			pass := ""
			if len(args) > 0 {
				pass = args[0]
			}

			passwordOK := false
			matchedHash := ""
			passwds, err := LoadPasswdFile(s.Config.PasswdFile)
			if err == nil {
				if hash, ok := passwds[s.User.Name]; ok {
					matchedHash = hash
					passwordOK = VerifyPassword(pass, hash)
				}
			}
			if !passwordOK && s.User.Password != "" {
				passwordOK = (s.User.Password == pass)
			}
			if !passwordOK {
				s.emitLoginFailure(s.User.Name, remoteIP, "bad_password")
				fmt.Fprintf(s.Conn, "530 Login incorrect.\r\n")
				return false
			}
			if matchedHash != "" {
				if upgraded, err := UpgradeLegacyPasswordHash(s.User.Name, pass, matchedHash, s.Config.PasswdFile); err != nil {
					if s.Config.Debug {
						log.Printf("[PASS] User %s legacy hash upgrade failed: %v", s.User.Name, err)
					}
				} else if upgraded && s.Config.Debug {
					log.Printf("[PASS] Upgraded legacy password hash to bcrypt for %s", s.User.Name)
				}
			}

			if !s.User.IPAllowed(remoteIP) {
				reason := "ip_not_allowed"
				if len(s.User.IPs) == 0 {
					reason = "no_ip_masks"
				}
				s.emitLoginFailure(s.User.Name, remoteIP, reason)
				fmt.Fprintf(s.Conn, "530 IP not allowed.\r\n")
				return false
			}

			if s.Config.PluginManager != nil {
				if err := s.Config.PluginManager.ValidateLogin(s.User, remoteIP); err != nil {
					if s.Config.Debug {
						log.Printf("[PASS] User %s rejected by plugin login policy: %v", s.User.Name, err)
					}
					s.emitLoginFailure(s.User.Name, remoteIP, "plugin_rejected")
					fmt.Fprintf(s.Conn, "530 %s.\r\n", err.Error())
					return false
				}
			}

			isTLSExempt := false
			for _, exemptUser := range s.Config.TLSExemptUsers {
				if exemptUser == s.User.Name {
					isTLSExempt = true
					break
				}
			}

			if s.Config.RequireTLSControl && !isTLSExempt && !s.IsTLS {
				if s.Config.Debug {
					log.Printf("[PASS] User %s rejected: TLS required on control channel", s.User.Name)
				}
				s.emitLoginFailure(s.User.Name, remoteIP, "tls_required")
				fmt.Fprintf(s.Conn, "530 TLS required.\r\n")
				return false
			}
			if s.User.LoginSlots > 0 {
				activeSessionsForUser := countSessionsForUser(s.User.Name)
				if activeSessionsForUser >= s.User.LoginSlots {
					s.emitLoginFailure(s.User.Name, remoteIP, "max_logins_reached")
					fmt.Fprintf(s.Conn, "530 Maximum simultaneous logins reached (%d).\r\n", s.User.LoginSlots)
					return false
				}
			}
			if s.User.PrimaryGroup != "" {
				if groupCfg, err := LoadGroupConfig(s.User.PrimaryGroup); err == nil && groupCfg.Simult > 0 {
					if countSessionsForGroup(s.User.PrimaryGroup) >= groupCfg.Simult {
						s.emitLoginFailure(s.User.Name, remoteIP, "group_simult_reached")
						fmt.Fprintf(s.Conn, "530 Group simultaneous login limit reached (%d).\r\n", groupCfg.Simult)
						return false
					}
				}
			}
			if strings.TrimSpace(s.User.CurrentDir) != "" {
				s.CurrentDir = path.Clean(s.User.CurrentDir)
			}
			s.User.LastLogin = time.Now().Unix()
			s.IsLogged = true
			s.PendingUser = ""
			s.PendingReason = ""
			fmt.Fprintf(s.Conn, "230-Welcome to GoFTPd, %s!\r\n", s.User.Name)
			fmt.Fprintf(s.Conn, "230-Tagline: %s\r\n", s.User.Tagline)

			s.showGlobalStats("230", false)
			fmt.Fprintf(s.Conn, "230 User logged in.\r\n")
			s.persistLoginStateAsync(s.User.Name, s.User.LastLogin)

		} else {
			remoteIP, _, _ := net.SplitHostPort(s.Conn.RemoteAddr().String())
			reason := s.PendingReason
			if reason == "" {
				reason = "unknown_user"
			}
			s.emitLoginFailure(s.PendingUser, remoteIP, reason)
			fmt.Fprintf(s.Conn, "530 Login incorrect.\r\n")
		}

	case "SYST":
		fmt.Fprintf(s.Conn, "215 UNIX Type: L8\r\n")

	case "TYPE":
		if len(args) == 0 {
			fmt.Fprintf(s.Conn, "501 Syntax error\r\n")
			return false
		}
		transferType, ok := normalizeTransferType(args[0])
		if !ok {
			fmt.Fprintf(s.Conn, "504 Command not implemented for that parameter.\r\n")
			return false
		}
		s.TransferType = transferType
		fmt.Fprintf(s.Conn, "200 Command OK\r\n")

	case "MODE":
		if len(args) == 0 {
			fmt.Fprintf(s.Conn, "501 Syntax error\r\n")
			return false
		}
		if _, ok := normalizeTransferMode(args[0]); !ok {
			fmt.Fprintf(s.Conn, "504 Command not implemented for that parameter.\r\n")
			return false
		}
		fmt.Fprintf(s.Conn, "200 Command OK\r\n")

	case "STRU":
		if len(args) == 0 {
			fmt.Fprintf(s.Conn, "501 Syntax error\r\n")
			return false
		}
		if _, ok := normalizeTransferStructure(args[0]); !ok {
			fmt.Fprintf(s.Conn, "504 Command not implemented for that parameter.\r\n")
			return false
		}
		fmt.Fprintf(s.Conn, "200 Command OK\r\n")

	case "REST":
		if len(args) == 0 {
			fmt.Fprintf(s.Conn, "501 Syntax error\r\n")
			return false
		}
		offset, err := strconv.ParseInt(strings.TrimSpace(args[0]), 10, 64)
		if err != nil || offset < 0 {
			fmt.Fprintf(s.Conn, "501 Invalid REST offset\r\n")
			return false
		}
		s.RestOffset = offset
		fmt.Fprintf(s.Conn, "350 REST position set.\r\n")

	case "PWD":
		fmt.Fprintf(s.Conn, "257 \"%s\" is current directory.\r\n", s.CurrentDir)

	case "CWD":
		target := "/"
		if len(args) > 0 {
			target = args[0]
		}
		if !strings.HasPrefix(target, "/") {
			target = path.Join(s.CurrentDir, target)
		}
		targetPath := path.Clean(target)
		if s.Config.Mode == "master" && s.MasterManager != nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				targetPath = path.Clean(bridge.ResolvePath(targetPath))
				parent := path.Dir(targetPath)
				name := path.Base(targetPath)
				if resolved := resolveKnownMarkerTarget(bridge, s.Config, parent, name); resolved != "" {
					targetPath = resolved
				}
				if targetPath != "/" {
					entry, ok := bridge.GetPathEntry(targetPath)
					if !ok {
						fmt.Fprintf(s.Conn, "550 %s: no such file or directory\r\n", targetPath)
						return false
					}
					if !entry.IsDir {
						fmt.Fprintf(s.Conn, "550 %s: not a directory\r\n", targetPath)
						return false
					}
				}
			}
		} else {
			localPath := filepath.Join(s.Config.StoragePath, filepath.FromSlash(strings.TrimPrefix(targetPath, "/")))
			info, err := os.Stat(localPath)
			if err != nil {
				if os.IsNotExist(err) {
					fmt.Fprintf(s.Conn, "550 %s: no such file or directory\r\n", targetPath)
				} else {
					fmt.Fprintf(s.Conn, "550 %s: %v\r\n", targetPath, err)
				}
				return false
			}
			if !info.IsDir() {
				fmt.Fprintf(s.Conn, "550 %s: not a directory\r\n", targetPath)
				return false
			}
		}
		if !s.canListPath(targetPath) {
			fmt.Fprintf(s.Conn, "550 %s: no such file or directory\r\n", targetPath)
			return false
		}
		s.CurrentDir = targetPath

		if s.Config.Mode == "master" && s.MasterManager != nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				emitCWDZipDIZInfo(s, bridge, s.CurrentDir)
				emitCWDAudioInfo(s, bridge, s.CurrentDir)
				if s.Config.ShowDiz != nil {
					for fileName, permission := range s.Config.ShowDiz {
						if fileName == ".message" {
							continue
						}
						if zipscript.ShowZipDIZOnCWDForDir(s.Config.Zipscript, s.CurrentDir) && strings.EqualFold(strings.TrimSpace(fileName), "file_id.diz") {
							continue
						}
						if permission == "*" || s.User.HasFlag(permission) {
							filePath := path.Join(s.CurrentDir, fileName)
							if content, err := bridge.ReadFile(filePath); err == nil && len(content) > 0 {
								text := strings.ReplaceAll(string(content), "\r\n", "\n")
								for _, line := range strings.Split(strings.TrimRight(text, "\n"), "\n") {
									fmt.Fprintf(s.Conn, "250-%s\r\n", line)
								}
							}
						}
					}
				}

				if raceStatusEligibleDir(s.CurrentDir) && zipscript.RaceStatsOnCWDForDir(s.Config.Zipscript, s.CurrentDir) {
					users, groups, totalBytes, present, total := bridge.GetVFSRaceStats(s.CurrentDir)
					users = trimRaceUsers(s.Config, users)
					groups = trimRaceGroups(s.Config, groups)

					if s.Config.Debug {
						log.Printf("[RACESTATS] dir=%s users=%d groups=%d totalBytes=%d present=%d total=%d",
							s.CurrentDir, len(users), len(groups), totalBytes, present, total)
					}

					if HasRaceStats(users, groups, totalBytes, present, total) {
						var builder strings.Builder
						RenderRaceStats(
							&builder,
							users,
							groups,
							totalBytes,
							present,
							total,
							s.Config.Version,
						)

						for _, line := range strings.Split(strings.TrimRight(builder.String(), "\r\n"), "\n") {
							fmt.Fprintf(s.Conn, "250-%s\r\n", line)
						}
					} else if s.Config.ShowCWDBanner {
						var builder strings.Builder
						RenderRaceHeader(&builder, s.Config.Version)
						for _, line := range strings.Split(strings.TrimRight(builder.String(), "\r\n"), "\n") {
							fmt.Fprintf(s.Conn, "250-%s\r\n", line)
						}
					}
				} else if s.Config.ShowCWDBanner {
					var builder strings.Builder
					RenderRaceHeader(&builder, s.Config.Version)
					for _, line := range strings.Split(strings.TrimRight(builder.String(), "\r\n"), "\n") {
						fmt.Fprintf(s.Conn, "250-%s\r\n", line)
					}
				}
			}
		}

		s.showGlobalStats("250", false)
		fmt.Fprintf(s.Conn, "250 Directory changed to %s\r\n", s.CurrentDir)

	case "CDUP":
		targetPath := path.Clean(path.Join(s.CurrentDir, ".."))
		if !s.canListPath(targetPath) {
			fmt.Fprintf(s.Conn, "550 %s: no such file or directory\r\n", targetPath)
			return false
		}
		s.CurrentDir = targetPath
		fmt.Fprintf(s.Conn, "250 Directory changed to %s\r\n", s.CurrentDir)

	case "MKD":
		if len(args) == 0 {
			fmt.Fprintf(s.Conn, "501 Syntax error\r\n")
			return false
		}

		requestedPath := args[0]
		var targetPath string
		if path.IsAbs(requestedPath) {
			targetPath = path.Clean(requestedPath)
		} else {
			targetPath = path.Join("/", s.CurrentDir, requestedPath)
		}

		if !path.IsAbs(targetPath) {
			targetPath = "/" + targetPath
		}
		targetPath = path.Clean(targetPath)

		aclPath := path.Join(s.Config.ACLBasePath, targetPath)
		if !s.ACLEngine.CanPerform(s.User, "MKD", aclPath) {
			fmt.Fprintf(s.Conn, "550 Access Denied: Insufficient flags.\r\n")
			return false
		}

		dirName := path.Base(targetPath)

		// Decide whether the new dir participates in dupe-checking. Skip:
		//  - section dirs (parent = root) — e.g. /TV-1080P, /X265, /MP3
		//  - known scene subfolders that exist inside many releases
		parent := path.Dir(targetPath)
		isSectionDir := parent == "/" || parent == "."
		isSubFolder := isSceneSubfolder(dirName)
		skipDupeCheck := s.ACLEngine != nil && s.ACLEngine.CanPerformRuleOnly(s.User, "NODUPECHECK", aclPath)
		dupeEligible := !isSectionDir && !isSubFolder && !skipDupeCheck

		if s.Config.PluginManager != nil {
			if err := s.Config.PluginManager.ValidateMKDir(s.User, targetPath); err != nil {
				fmt.Fprintf(s.Conn, "550 %v\r\n", err)
				return false
			}
		}

		if dupeEligible && s.DupeChecker != nil {
			if dc, ok := s.DupeChecker.(interface{ IsDupe(string) (bool, error) }); ok {
				if isDupe, err := dc.IsDupe(dirName); err == nil && isDupe {
					fmt.Fprintf(s.Conn, "550 %s: directory already exists in dupe database.\r\n", dirName)
					return false
				}
			}
		}

		if s.Config.Mode == "master" && s.MasterManager != nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				if bridge.FileExists(targetPath) {
					fmt.Fprintf(s.Conn, "550 %s: directory already exists.\r\n", dirName)
					return false
				}
				if err := bridge.MakeDir(targetPath, s.User.Name, s.User.PrimaryGroup); err != nil {
					fmt.Fprintf(s.Conn, "550 MKD failed: %v\r\n", err)
					return false
				}
			}
		}

		if dupeEligible && s.DupeChecker != nil {
			if dc, ok := s.DupeChecker.(interface {
				AddDupe(string, string, string, int, int64) error
			}); ok {
				dc.AddDupe(dirName, s.User.PrimaryGroup, s.User.Name, 0, 0)
			}
		}

		s.emitEvent(EventMKDir, targetPath, dirName, 0, 0, nil)

		fmt.Fprintf(s.Conn, "257 \"%s\" created\r\n", targetPath)

	case "RMD":
		if len(args) == 0 {
			fmt.Fprintf(s.Conn, "501 Syntax error\r\n")
			return false
		}
		dirPath := path.Join(s.CurrentDir, args[0])
		if !s.canRemoveDirPath(dirPath) {
			fmt.Fprintf(s.Conn, "550 Access Denied: Insufficient flags.\r\n")
			return false
		}
		if s.Config.Mode == "master" && s.MasterManager != nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				if err := cleanupAudioSortLinksForRelease(bridge, s.Config.Zipscript, dirPath); err != nil && s.Config.Debug {
					log.Printf("[MASTER-ZS] audio sort cleanup skipped for %s: %v", dirPath, err)
				}
				if err := bridge.DeleteFile(dirPath); err != nil {
					fmt.Fprintf(s.Conn, "550 Delete failed: %v\r\n", err)
					return false
				}
			}
		}
		s.emitEvent(EventRMDir, path.Join(s.CurrentDir, args[0]), args[0], 0, 0, nil)
		fmt.Fprintf(s.Conn, "250 Directory removed.\r\n")

	case "SIZE":
		if len(args) == 0 {
			fmt.Fprintf(s.Conn, "501 Syntax error\r\n")
			return false
		}
		if s.Config.Mode == "master" && s.MasterManager != nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				filePath := path.Join(s.CurrentDir, args[0])
				size := bridge.GetFileSize(filePath)
				if size >= 0 {
					fmt.Fprintf(s.Conn, "213 %d\r\n", size)
				} else {
					fmt.Fprintf(s.Conn, "550 File not found.\r\n")
				}
			}
		}

	case "MDTM":
		if len(args) == 0 {
			fmt.Fprintf(s.Conn, "501 Syntax error\r\n")
			return false
		}
		if s.Config.Mode == "master" && s.MasterManager != nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				entries := bridge.ListDir(s.CurrentDir)
				found := false
				for _, e := range entries {
					if e.Name == args[0] {
						fmt.Fprintf(s.Conn, "213 %s\r\n", timeutil.FTPMachineUnix(e.ModTime))
						found = true
						break
					}
				}
				if !found {
					fmt.Fprintf(s.Conn, "550 File not found.\r\n")
				}
			}
		}

	case "DELE":
		if len(args) == 0 {
			fmt.Fprintf(s.Conn, "501 Syntax error\r\n")
			return false
		}
		filePath := path.Join(s.CurrentDir, args[0])
		if !s.canDeletePath(filePath) {
			fmt.Fprintf(s.Conn, "550 Access Denied: Insufficient flags.\r\n")
			return false
		}
		if s.Config.Mode == "master" && s.MasterManager != nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				releasePath := path.Clean(path.Dir(filePath))
				previousMedia := cloneStringMap(bridge.GetDirMediaInfo(releasePath))
				if err := bridge.DeleteFile(filePath); err != nil {
					fmt.Fprintf(s.Conn, "550 Delete failed: %v\r\n", err)
					return false
				}
				if zipscript.IsMediaInfoFile(path.Base(filePath)) {
					if err := maybeRefreshReleaseMediaInfoAndLinks(s.Config, bridge, releasePath, previousMedia); err != nil && s.Config.Debug {
						log.Printf("[MASTER-ZS] media cleanup skipped for %s: %v", releasePath, err)
					}
				}
			}
		}
		fmt.Fprintf(s.Conn, "250 File deleted.\r\n")

	case "RNFR":
		if len(args) == 0 {
			fmt.Fprintf(s.Conn, "501 Syntax error\r\n")
			return false
		}
		s.RenameFrom = args[0]
		fmt.Fprintf(s.Conn, "350 File exists, ready for destination name.\r\n")

	case "RNTO":
		if len(args) == 0 || s.RenameFrom == "" {
			fmt.Fprintf(s.Conn, "503 Bad sequence of commands.\r\n")
			return false
		}
		fromPath := path.Join(s.CurrentDir, s.RenameFrom)
		toPath := path.Join(s.CurrentDir, args[0])
		if !s.canRenamePath(fromPath, toPath) {
			fmt.Fprintf(s.Conn, "550 Access Denied: Insufficient flags.\r\n")
			return false
		}
		if s.Config.Mode == "master" && s.MasterManager != nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				toDir := s.CurrentDir
				toName := args[0]
				fromRelease := path.Clean(path.Dir(fromPath))
				previousMedia := cloneStringMap(bridge.GetDirMediaInfo(fromRelease))
				if err := bridge.RenameFile(fromPath, toDir, toName); err != nil {
					fmt.Fprintf(s.Conn, "550 Rename failed: %v\r\n", err)
					s.RenameFrom = ""
					return false
				}
				if zipscript.IsMediaInfoFile(path.Base(fromPath)) || zipscript.IsMediaInfoFile(path.Base(toPath)) {
					if err := maybeRefreshReleaseMediaInfoAndLinks(s.Config, bridge, fromRelease, previousMedia); err != nil && s.Config.Debug {
						log.Printf("[MASTER-ZS] media cleanup skipped for %s: %v", fromRelease, err)
					}
				}
			}
		}
		fmt.Fprintf(s.Conn, "250 Rename successful.\r\n")
		s.RenameFrom = ""

	case "SITE":
		return s.DispatchSiteCommand(args)

	case "PRET":
		if s.Config.Debug && len(args) > 0 {
			log.Printf("[PRET] Client preparing for %s", args[0])
		}
		s.clearPreparedTransferState()
		if len(args) == 0 {
			fmt.Fprintf(s.Conn, "501 Syntax error.\r\n")
			return false
		}
		preparedCmd, preparedArg, err := validatePretRequest(s, args[0], args[1:])
		if err != nil {
			fmt.Fprintf(s.Conn, "504 %s\r\n", err.Error())
			return false
		}
		if s.Config.XdupeEnabled && preparedCmd == "STOR" && preparedArg != "" && s.Config.Mode == "master" && s.MasterManager != nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				pretTarget := path.Clean(path.Join(s.CurrentDir, preparedArg))
				if path.IsAbs(strings.TrimSpace(preparedArg)) {
					pretTarget = path.Clean(preparedArg)
				}
				pretTarget = bridge.ResolvePath(pretTarget)
				if bridge.FileExists(pretTarget) {
					for _, line := range xdupeResponseLines(s.XDupeMode, existingFileNamesForXDupe(bridge.ListDir(path.Dir(pretTarget)))) {
						fmt.Fprintf(s.Conn, "553-%s\r\n", line)
					}
					fmt.Fprintf(s.Conn, "553 %s: file already exists (X-DUPE)\r\n", path.Base(pretTarget))
					return false
				}
			}
		}
		s.PretCmd = preparedCmd
		s.PretArg = preparedArg
		fmt.Fprintf(s.Conn, "200 %s\r\n", pretSuccessMessage(preparedCmd))
		return false

	case "ABOR":
		s.abortCurrentTransfer("Transfer aborted")
		fmt.Fprintf(s.Conn, "226 Abort successful\r\n")
		return false

	case "NOOP":
		fmt.Fprintf(s.Conn, "200 NOOP OK\r\n")
		return false

	case "PASV":
		if s.Config.Debug {
			log.Printf("[PASV] Starting passive mode setup (pret=%s, passthrough=%v)", s.PretCmd, s.Config.Passthrough)
		}
		s.clearActiveTransferSetup()
		if !hasPretForPassive(s) {
			s.clearPreparedTransferState()
			fmt.Fprintf(s.Conn, "500 You need to use a client supporting PRET (PRE Transfer) to use PASV\r\n")
			return false
		}

		if s.Config.Passthrough && s.Config.Mode == "master" && s.MasterManager != nil {
			if s.PretCmd == "STOR" || s.PretCmd == "RETR" {
				if bridge, ok := s.MasterManager.(MasterBridge); ok {
					targetPath := s.resolvePretTargetPath(bridge)

					var slaveIP string
					var port int
					var xferIdx int32
					var slaveName string
					var err error
					if s.PretCmd == "RETR" {
						slaveIP, port, xferIdx, slaveName, err = bridge.SlaveListenForDownloadPassthrough(targetPath, s.DataTLS, false)
					} else {
						slaveIP, port, xferIdx, slaveName, err = bridge.SlaveListenForPassthrough(targetPath, s.DataTLS, false)
					}
					if err != nil {
						log.Printf("[PASV] Passthrough slave listen failed: %v", err)
						fmt.Fprintf(s.Conn, "450 No available slave for upcoming transfer.\r\n")
						return false
					}
					s.PassthruSlave = slaveName
					s.PassthruXferIdx = xferIdx
					if s.DataListen != nil {
						s.DataListen.Close()
						s.DataListen = nil
					}
					ip, err := ftpPassiveIPv4(slaveIP)
					if err != nil {
						fmt.Fprintf(s.Conn, "500 Invalid passive address for selected slave.\r\n")
						s.clearPassiveTransferSetup()
						return false
					}
					response := fmt.Sprintf("227 Entering Passive Mode (%s,%d,%d)\r\n", ip, port/256, port%256)
					if s.Config.Debug {
						log.Printf("[PASV] Passthrough to slave %s: %s (port: %d, xferIdx: %d)", slaveName, strings.TrimSpace(response), port, xferIdx)
					}
					fmt.Fprintf(s.Conn, response)
					return false
				}
			}
		}

		var l net.Listener
		var port int
		var err error
		for p := s.Config.PasvMin; p <= s.Config.PasvMax; p++ {
			l, err = net.Listen("tcp", fmt.Sprintf(":%d", p))
			if err == nil {
				port = p
				break
			}
		}
		if l == nil {
			if s.Config.Debug {
				log.Printf("[PASV] No available ports (tried %d-%d)", s.Config.PasvMin, s.Config.PasvMax)
			}
			fmt.Fprintf(s.Conn, "425 Can't open passive data connection.\r\n")
			return false
		}
		s.DataListen = l
		s.PassthruSlave = nil
		s.PassthruXferIdx = 0
		s.nextDataTLSClientMode = false
		ip, err := ftpPassiveIPv4(s.Config.PublicIP)
		if err != nil {
			fmt.Fprintf(s.Conn, "500 Invalid passive address configuration.\r\n")
			if s.DataListen != nil {
				s.DataListen.Close()
				s.DataListen = nil
			}
			return false
		}
		response := fmt.Sprintf("227 Entering Passive Mode (%s,%d,%d)\r\n", ip, port/256, port%256)
		if s.Config.Debug {
			log.Printf("[PASV] Sending response: %s (port: %d)", strings.TrimSpace(response), port)
		}
		fmt.Fprintf(s.Conn, response)
		return false

	case "PORT":
		if len(args) == 0 {
			return false
		}
		s.clearPassiveTransferSetup()
		ip, port, err := parsePortTarget(args[0])
		if err != nil {
			fmt.Fprintf(s.Conn, "501 Syntax error\r\n")
			return false
		}
		controlHost, _, splitErr := net.SplitHostPort(s.Conn.RemoteAddr().String())
		controlIP := net.ParseIP(controlHost)
		if splitErr == nil && shouldRejectPortTarget(controlIP, ip) {
			fmt.Fprintf(s.Conn, "501 ==YOU'RE BEHIND A NAT ROUTER==\r\n")
			fmt.Fprintf(s.Conn, "501 Configure your FTP client to use your real IP: %s\r\n", controlHost)
			fmt.Fprintf(s.Conn, "501 Or use a PRET-capable passive transfer mode.\r\n")
			return false
		}
		s.ActiveAddr = fmt.Sprintf("%s:%d", ip.String(), port)
		warnings := portTargetWarnings(controlIP, ip)
		if len(warnings) == 0 {
			fmt.Fprintf(s.Conn, "200 PORT command successful.\r\n")
			return false
		}
		for _, warning := range warnings {
			fmt.Fprintf(s.Conn, "200-%s\r\n", warning)
		}
		fmt.Fprintf(s.Conn, "200 PORT command successful.\r\n")

	case "MLST":
		target := s.CurrentDir
		if len(args) > 0 && strings.TrimSpace(args[0]) != "" {
			t := strings.TrimSpace(args[0])
			if strings.HasPrefix(t, "/") {
				target = path.Clean(t)
			} else {
				target = path.Clean(path.Join(s.CurrentDir, t))
			}
		}
		if !s.canListPath(target) {
			fmt.Fprintf(s.Conn, "550 %s: no such file or directory\r\n", target)
			return false
		}

		facts := ""
		found := false
		if s.Config.Mode == "master" && s.MasterManager != nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				if target == "/" {
					facts = "Type=dir;Perm=elcmp; /"
					found = true
				} else {
					parent := path.Dir(target)
					name := path.Base(target)
					for _, e := range bridge.ListDir(parent) {
						if e.Name == name {
							aclPath := path.Join(s.Config.ACLBasePath, parent, e.Name)
							if !s.ACLEngine.CanPerform(s.User, "LIST", aclPath) {
								break
							}
							ts := timeutil.FTPMachineUnix(e.ModTime)
							var parts []string
							if e.IsSymlink {
								parts = []string{
									fmt.Sprintf("Modify=%s", ts),
									"Perm=el",
									"Type=" + mlsdSymlinkType(e),
								}
							} else if e.IsDir {
								perm := "elcmp"
								if e.Mode == 0555 {
									perm = "el"
								}
								parts = []string{
									fmt.Sprintf("Modify=%s", ts),
									"Perm=" + perm,
									"Type=dir",
								}
							} else {
								parts = []string{
									fmt.Sprintf("Modify=%s", ts),
									"Perm=radfw",
									"Type=file",
									fmt.Sprintf("Size=%d", e.Size),
								}
							}
							facts = strings.Join(parts, ";") + "; " + target
							found = true
							break
						}
					}
				}
			}
		}

		if !found {
			fmt.Fprintf(s.Conn, "550 %s: no such file or directory\r\n", target)
			return false
		}
		fmt.Fprintf(s.Conn, "250- Listing %s\r\n", target)
		fmt.Fprintf(s.Conn, " %s\r\n", facts)
		fmt.Fprintf(s.Conn, "250 End\r\n")

	case "MLSD":
		defer s.clearPreparedTransferState()
		if !s.hasPreparedDataConnection() {
			fmt.Fprintf(s.Conn, "503 Bad sequence of commands.\r\n")
			return false
		}
		targetPath := s.CurrentDir
		if s.Config.Mode == "master" && s.MasterManager != nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				targetPath = s.resolveListTargetPath("MLSD", args, bridge)
			}
		} else {
			targetPath = s.resolveListTargetPath("MLSD", args, nil)
		}
		if err := s.validateListDirectoryTarget(targetPath, s.masterBridgeOrNil()); err != nil {
			if err.Error() == "504" {
				fmt.Fprintf(s.Conn, "504 Command not implemented for that parameter.\r\n")
			} else {
				fmt.Fprintf(s.Conn, "550 %s: no such file or directory\r\n", targetPath)
			}
			return false
		}
		if s.Config.Debug {
			log.Printf("[MLSD] Client requesting machine list for %s", targetPath)
		}
		fmt.Fprintf(s.Conn, "150 File status okay; about to open data connection.\r\n")

		raw, err := s.getRawDataConn()
		if err != nil {
			fmt.Fprintf(s.Conn, "425 Data connection failed\r\n")
			return false
		}
		dataConn, err := s.upgradeDataTLS(raw, tlsConfig)
		if err != nil {
			raw.Close()
			fmt.Fprintf(s.Conn, "435 Failed TLS negotiation on data channel\r\n")
			return false
		}

		var output strings.Builder

		if s.Config.Mode == "master" && s.MasterManager != nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				entries := bridge.ListDir(targetPath)

				// Keep MLSD rich for cbftp, but only from already-available state.
				siteName := s.Config.SiteNameShort
				if siteName == "" {
					siteName = "GoFTPd"
				}
				if zipscript.ShowStatusBarForDir(s.Config.Zipscript, targetPath) {
					if statusName := dirRaceStatusName(bridge, s.Config, targetPath, siteName); strings.TrimSpace(statusName) != "" {
						nowTs := timeutil.FTPMachine(time.Now())
						entryType := "file"
						if zipscript.StatusBarDirectoryForDir(s.Config.Zipscript, targetPath) {
							entryType = "dir"
						}
						output.WriteString(fmt.Sprintf("Modify=%s;Perm=el;Type=%s; %s\r\n", nowTs, entryType, statusName))
					}
				}

				for _, e := range entries {
					if strings.HasPrefix(e.Name, ".") {
						continue
					}
					aclPath := path.Join(s.Config.ACLBasePath, targetPath, e.Name)
					if !s.ACLEngine.CanPerform(s.User, "LIST", aclPath) {
						continue
					}
					ts := timeutil.FTPMachineUnix(e.ModTime)
					var perm string
					var facts []string
					if e.IsSymlink {
						perm = "el"
						facts = []string{
							fmt.Sprintf("Modify=%s", ts),
							"Perm=" + perm,
							"Type=" + mlsdSymlinkType(e),
						}
					} else if e.IsDir {
						perm = "elcmp" // enter, list, create, mkdir, purge
						if e.Mode == 0555 {
							perm = "el"
						}
						facts = []string{
							fmt.Sprintf("Modify=%s", ts),
							"Perm=" + perm,
							"Type=dir",
						}
					} else {
						perm = "radfw" // read, append, delete, rename, write
						facts = []string{
							fmt.Sprintf("Modify=%s", ts),
							"Perm=" + perm,
							"Type=file",
							fmt.Sprintf("Size=%d", e.Size),
						}
					}
					output.WriteString(strings.Join(facts, ";") + "; " + e.Name + "\r\n")
				}
			}
		} else {
			mlsdPath := filepath.Join(s.Config.StoragePath, targetPath)
			files, err := os.ReadDir(mlsdPath)
			if err != nil {
				if s.Config.Debug {
					log.Printf("[MLSD] ReadDir %s: %v", mlsdPath, err)
				}
			}
			for _, f := range files {
				if strings.HasPrefix(f.Name(), ".") {
					continue
				}
				if !s.Config.ShowSymlinks && f.Type()&fs.ModeSymlink != 0 {
					continue
				}
				if !s.canListPath(path.Join(targetPath, f.Name())) {
					continue
				}
				fileName := f.Name()
				fullPath := filepath.Join(mlsdPath, fileName)
				isSymlink := f.Type()&fs.ModeSymlink != 0
				var info os.FileInfo
				if isSymlink {
					info, err = os.Lstat(fullPath)
				} else {
					info, err = f.Info()
				}
				if err != nil || info == nil {
					continue
				}
				facts := []string{
					fmt.Sprintf("Modify=%s", timeutil.FTPMachine(info.ModTime())),
					fmt.Sprintf("Perm=%s", getMlsdPerm(info, isSymlink)),
				}
				if isSymlink {
					facts = append(facts, "Type=OS.unix=symlink")
				} else if info.IsDir() {
					facts = append(facts, "Type=dir")
				} else {
					facts = append(facts, "Type=file")
					facts = append(facts, fmt.Sprintf("Size=%d", info.Size()))
				}
				output.WriteString(strings.Join(facts, ";") + "; " + fileName + "\r\n")
			}
		}

		dataConn.Write([]byte(output.String()))
		dataConn.Close()
		fmt.Fprintf(s.Conn, "226 Directory listing complete.\r\n")
		return false

	case "NLST":
		defer s.clearPreparedTransferState()
		if !s.hasPreparedDataConnection() {
			fmt.Fprintf(s.Conn, "503 Bad sequence of commands.\r\n")
			return false
		}
		targetPath := s.CurrentDir
		if s.Config.Mode == "master" && s.MasterManager != nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				targetPath = s.resolveListTargetPath("NLST", args, bridge)
			}
		} else {
			targetPath = s.resolveListTargetPath("NLST", args, nil)
		}
		if err := s.validateListTargetExists(targetPath, s.masterBridgeOrNil()); err != nil {
			fmt.Fprintf(s.Conn, "550 %s: no such file or directory\r\n", targetPath)
			return false
		}
		fmt.Fprintf(s.Conn, "150 Opening ASCII mode data connection.\r\n")

		raw, err := s.getRawDataConn()
		if err != nil {
			fmt.Fprintf(s.Conn, "425 Data connection failed\r\n")
			return false
		}
		dataConn, err := s.upgradeDataTLS(raw, tlsConfig)
		if err != nil {
			raw.Close()
			fmt.Fprintf(s.Conn, "435 Failed TLS negotiation on data channel\r\n")
			return false
		}

		var output strings.Builder

		if s.Config.Mode == "master" && s.MasterManager != nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				if entry, found := bridge.GetPathEntry(targetPath); found && !entry.IsDir {
					aclPath := path.Join(s.Config.ACLBasePath, targetPath)
					if s.ACLEngine.CanPerform(s.User, "LIST", aclPath) && !strings.HasPrefix(entry.Name, ".") {
						output.WriteString(entry.Name + "\r\n")
					}
				} else {
					for _, e := range bridge.ListDir(targetPath) {
						if strings.HasPrefix(e.Name, ".") {
							continue
						}
						if strings.HasPrefix(e.Name, "[#") || strings.HasPrefix(e.Name, "[:") {
							continue
						}
						if strings.Contains(e.Name, "COMPLETE") && strings.Contains(e.Name, "[") {
							continue
						}
						aclPath := path.Join(s.Config.ACLBasePath, targetPath, e.Name)
						if !s.ACLEngine.CanPerform(s.User, "LIST", aclPath) {
							continue
						}
						output.WriteString(e.Name + "\r\n")
					}
				}
			}
		} else {
			listPath := filepath.Join(s.Config.StoragePath, targetPath)
			if info, statErr := os.Lstat(listPath); statErr == nil && !info.IsDir() {
				name := filepath.Base(listPath)
				if !strings.HasPrefix(name, ".") && s.canListPath(targetPath) {
					output.WriteString(name + "\r\n")
				}
			} else if files, readErr := os.ReadDir(listPath); readErr == nil {
				for _, f := range files {
					if strings.HasPrefix(f.Name(), ".") {
						continue
					}
					if !s.Config.ShowSymlinks && f.Type()&fs.ModeSymlink != 0 {
						continue
					}
					if !s.canListPath(path.Join(targetPath, f.Name())) {
						continue
					}
					output.WriteString(f.Name() + "\r\n")
				}
			}
		}

		dataConn.Write([]byte(output.String()))
		dataConn.Close()

		if s.Config.Mode == "master" && s.MasterManager != nil {
			s.showGlobalStats("226", false)
		}

		fmt.Fprintf(s.Conn, "226 Directory listing complete.\r\n")
		return false

	case "LIST":
		defer s.clearPreparedTransferState()
		if !s.hasPreparedDataConnection() {
			fmt.Fprintf(s.Conn, "503 Bad sequence of commands.\r\n")
			return false
		}
		targetPath := s.CurrentDir
		if s.Config.Mode == "master" && s.MasterManager != nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				targetPath = s.resolveListTargetPath("LIST", args, bridge)
			}
		} else {
			targetPath = s.resolveListTargetPath("LIST", args, nil)
		}
		if err := s.validateListDirectoryTarget(targetPath, s.masterBridgeOrNil()); err != nil {
			if err.Error() == "504" {
				fmt.Fprintf(s.Conn, "504 Command not implemented for that parameter.\r\n")
			} else {
				fmt.Fprintf(s.Conn, "550 %s: no such file or directory\r\n", targetPath)
			}
			return false
		}
		fmt.Fprintf(s.Conn, "150 Opening ASCII mode data connection.\r\n")

		raw, err := s.getRawDataConn()
		if err != nil {
			fmt.Fprintf(s.Conn, "425 Data connection failed\r\n")
			return false
		}
		dataConn, err := s.upgradeDataTLS(raw, tlsConfig)
		if err != nil {
			raw.Close()
			fmt.Fprintf(s.Conn, "435 Failed TLS negotiation on data channel\r\n")
			return false
		}

		var output strings.Builder

		if s.Config.Mode == "master" && s.MasterManager != nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				entries := bridge.ListDir(targetPath)
				now := timeutil.Now().Format("Jan _2 15:04")
				siteName := s.Config.SiteNameShort
				if siteName == "" {
					siteName = "GoFTPd"
				}

				totalBytes, present, total := dirRaceProgress(bridge, s.Config, targetPath)
				if s.Config.Debug {
					log.Printf("[LIST/RACESTATS] dir=%s totalBytes=%d present=%d total=%d",
						targetPath, totalBytes, present, total)
				}

				existingFiles := make(map[string]bool)
				for _, e := range entries {
					existingFiles[e.Name] = true
				}

				if zipscript.ShowStatusBarForDir(s.Config.Zipscript, targetPath) {
					if statusName := dirRaceStatusName(bridge, s.Config, targetPath, siteName); strings.TrimSpace(statusName) != "" {
						mode := "drwxr-xr-x"
						size := "4096"
						if !zipscript.StatusBarDirectoryForDir(s.Config.Zipscript, targetPath) {
							mode = "-rw-r--r--"
							size = "0"
						}
						output.WriteString(fmt.Sprintf("%s   1 %-8s %-8s %10s %s %s\r\n",
							mode, "GoFTPd", "GoFTPd", size, now, statusName))
					}
				}

				for _, e := range entries {
					if strings.HasPrefix(e.Name, ".") {
						continue
					}
					if strings.HasPrefix(e.Name, "[#") || strings.HasPrefix(e.Name, "[:") {
						continue
					}
					if strings.Contains(e.Name, "COMPLETE") && strings.Contains(e.Name, "[") {
						continue
					}

					aclPath := path.Join(s.Config.ACLBasePath, targetPath, e.Name)
					if !s.ACLEngine.CanPerform(s.User, "LIST", aclPath) {
						continue
					}

					mode := ftpListMode(e)
					size := fmt.Sprintf("%d", e.Size)
					name := e.Name
					if e.IsSymlink {
						size = "0"
						name = fmt.Sprintf("%s -> %s", e.Name, e.LinkTarget)
					} else if e.IsDir {
						size = "4096"
					}
					ts := timeutil.Unix(e.ModTime).Format("Jan _2 15:04")
					owner := "GoFTPd"
					group := "GoFTPd"
					if s.Config.ShowRealOwnerGroup {
						owner = strings.TrimSpace(e.Owner)
						group = strings.TrimSpace(e.Group)
						if owner == "" {
							owner = "GoFTPd"
						}
						if group == "" {
							group = "GoFTPd"
						}
					}
					output.WriteString(fmt.Sprintf("%s   1 %-8s %-8s %10s %s %s\r\n",
						mode, owner, group, size, ts, name))
				}

			}
		} else {
			// FALLBACK: Standalone mode directory listing for cbftp
			listPath := filepath.Join(s.Config.StoragePath, targetPath)
			files, err := os.ReadDir(listPath)
			if err == nil {

				for _, f := range files {
					if strings.HasPrefix(f.Name(), ".") {
						continue
					}
					if !s.Config.ShowSymlinks && f.Type()&fs.ModeSymlink != 0 {
						continue
					}
					if !s.canListPath(path.Join(targetPath, f.Name())) {
						continue
					}
					info, err := f.Info()
					if err != nil {
						continue
					}
					mode := "-rw-r--r--"
					size := fmt.Sprintf("%d", info.Size())
					if info.IsDir() {
						mode = "drwxr-xr-x"
						size = "4096"
					} else if f.Type()&fs.ModeSymlink != 0 {
						mode = "lrwxrwxrwx"
					}
					ts := timeutil.In(info.ModTime()).Format("Jan _2 15:04")
					output.WriteString(fmt.Sprintf("%s   1 %-8s %-8s %10s %s %s\r\n",
						mode, "GoFTPd", "GoFTPd", size, ts, f.Name()))
				}
			}
		}

		dataConn.Write([]byte(output.String()))
		dataConn.Close()

		// Only show stats in master mode so we don't crash standalone
		if s.Config.Mode == "master" && s.MasterManager != nil {
			s.showGlobalStats("226", false)
		}

		fmt.Fprintf(s.Conn, "226 Directory listing complete.\r\n")
		return false

	case "STOR":
		if len(args) == 0 {
			return false
		}
		defer s.clearPreparedTransferState()
		if !s.hasPreparedTransferChannel() {
			fmt.Fprintf(s.Conn, "503 Bad sequence of commands.\r\n")
			return false
		}

		isTLSExempt := false
		for _, exemptUser := range s.Config.TLSExemptUsers {
			if exemptUser == s.User.Name {
				isTLSExempt = true
				break
			}
		}
		if s.Config.RequireTLSData && !isTLSExempt && !s.DataTLS {
			fmt.Fprintf(s.Conn, "550 TLS required for data transfers.\r\n")
			return false
		}

		fileName := args[0]
		restOffset := s.RestOffset
		s.RestOffset = 0
		var existingNames []string
		var masterUploadEntries []MasterFileEntry
		masterUploadEntriesLoaded := false
		uploadDir := s.CurrentDir
		uploadPath := path.Clean(path.Join(s.CurrentDir, fileName))
		if path.IsAbs(strings.TrimSpace(fileName)) {
			uploadPath = path.Clean(fileName)
		}
		if s.Config.Mode == "master" && s.MasterManager != nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				uploadPath = bridge.ResolvePath(uploadPath)
			}
		}
		uploadDir = path.Dir(uploadPath)
		fileName = path.Base(uploadPath)
		getMasterUploadEntries := func(bridge MasterBridge) []MasterFileEntry {
			if !masterUploadEntriesLoaded {
				masterUploadEntries = bridge.ListDir(uploadDir)
				masterUploadEntriesLoaded = true
			}
			return masterUploadEntries
		}

		aclPath := path.Join(s.Config.ACLBasePath, uploadPath)
		if !s.ACLEngine.CanPerform(s.User, "UPLOAD", aclPath) {
			fmt.Fprintf(s.Conn, "550 Access Denied: Cannot upload here.\r\n")
			return false
		}

		if s.Config.Mode == "master" && s.MasterManager != nil {
			fileExists := false
			var xdupeNames []string
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				if err := ensureUploadDirsForEvent(s, bridge, uploadDir); err != nil {
					fmt.Fprintf(s.Conn, "550 Upload prepare failed: %v\r\n", err)
					return false
				}
				fileExists = bridge.FileExists(uploadPath)
				if s.Config.XdupeEnabled {
					xdupeNames = existingFileNamesForXDupe(getMasterUploadEntries(bridge))
				}
			}
			if fileExists && restOffset == 0 {
				if s.Config.XdupeEnabled {
					for _, line := range xdupeResponseLines(s.XDupeMode, xdupeNames) {
						fmt.Fprintf(s.Conn, "553-%s\r\n", line)
					}
					fmt.Fprintf(s.Conn, "553 %s: file already exists (X-DUPE)\r\n", fileName)
				} else {
					fmt.Fprintf(s.Conn, "553 %s: file already exists\r\n", fileName)
				}
				return false
			}
		}
		if restOffset > 0 {
			if !zipscript.AllowResumeForDir(s.Config.Zipscript, uploadDir) {
				fmt.Fprintf(s.Conn, "550 Resume is disabled for this release.\r\n")
				return false
			}
			if s.Config.Mode == "master" && s.MasterManager != nil {
				if bridge, ok := s.MasterManager.(MasterBridge); ok {
					size := bridge.GetFileSize(uploadPath)
					if size < 0 {
						fmt.Fprintf(s.Conn, "550 Resume target not found.\r\n")
						return false
					}
					if restOffset > size {
						fmt.Fprintf(s.Conn, "550 Resume offset beyond end of file.\r\n")
						return false
					}
				}
			}
		}
		if s.Config.Mode == "master" && s.MasterManager != nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				entries := getMasterUploadEntries(bridge)
				existingNames = zipscriptExistingNamesFromEntries(entries)
				existingDirs := zipscriptExistingDirNamesFromEntries(entries)
				if shouldBlockZipDIZUpload(s.Config, uploadDir, fileName) {
					fmt.Fprintf(s.Conn, "550 zipscript: upload file_id.diz inside the zip, not as a standalone file\r\n")
					return false
				}
				if err := zipscript.ValidateUpload(s.Config.Zipscript, s.User, uploadDir, fileName, existingNames, existingDirs, bridge.GetSFVData(uploadDir)); err != nil {
					fmt.Fprintf(s.Conn, "550 %s\r\n", err)
					return false
				}
			}
		}
		localPath := filepath.Join(s.Config.StoragePath, filepath.FromSlash(strings.TrimPrefix(uploadPath, "/")))
		if s.Config.Mode != "master" || s.MasterManager == nil {
			if shouldBlockZipDIZUpload(s.Config, uploadDir, fileName) {
				fmt.Fprintf(s.Conn, "550 zipscript: upload file_id.diz inside the zip, not as a standalone file\r\n")
				return false
			}
			dirEntries, err := os.ReadDir(filepath.Dir(localPath))
			if err == nil {
				localExistingNames := make([]string, 0, len(dirEntries))
				localExistingDirs := make([]string, 0, len(dirEntries))
				for _, entry := range dirEntries {
					localExistingNames = append(localExistingNames, entry.Name())
					if entry.IsDir() {
						localExistingDirs = append(localExistingDirs, entry.Name())
					}
				}
				if err := zipscript.ValidateUpload(s.Config.Zipscript, s.User, uploadDir, fileName, localExistingNames, localExistingDirs, localSFVEntriesForDir(filepath.Dir(localPath))); err != nil {
					fmt.Fprintf(s.Conn, "550 %s\r\n", err)
					return false
				}
			}
		}
		if s.User != nil && s.User.UploadSlots > 0 {
			activeUploads := countTransfersForUser(s.User.Name, "upload")
			if activeUploads >= s.User.UploadSlots {
				fmt.Fprintf(s.Conn, "550 Maximum simultaneous uploads reached (%d).\r\n", s.User.UploadSlots)
				return false
			}
		}
		if s.User != nil && s.User.MaxSim > 0 {
			activeTransfers := countTransfersForUserAllDirections(s.User.Name)
			if activeTransfers >= s.User.MaxSim {
				fmt.Fprintf(s.Conn, "550 Maximum simultaneous transfers reached (%d).\r\n", s.User.MaxSim)
				return false
			}
		}
		if s.Config.Passthrough && s.Config.Mode == "master" && s.MasterManager != nil && s.ActiveAddr != "" && s.PassthruSlave == nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				filePath := uploadPath
				portAddr := s.ActiveAddr
				s.ActiveAddr = ""

				log.Printf("[Passthrough] PORT STOR %s → slave connects to %s", filePath, portAddr)
				fmt.Fprintf(s.Conn, "150 Opening %s mode data connection.\r\n", transferTypeReplyName(s.TransferType))
				s.beginTransfer("upload", filePath)
				defer s.endTransfer()

				fileSize, checksum, xferMs, err := bridge.SlaveConnectAndReceive(filePath, portAddr, s.User.Name, s.User.PrimaryGroup, restOffset, s.DataTLS, s.SSCN, s.currentTransferTypeByte())
				_ = xferMs

				if err != nil {
					if writeDuplicateUploadResponse(s, bridge, uploadDir, fileName, err) {
						return false
					}
					if s.Config != nil && s.Config.Debug {
						log.Printf("[Passthrough] PORT upload failed for user %s path %s: %s", s.User.Name, filePath, formatTransferFailureLog(err))
					}
					maybeHandleSlowTransfer(s, "upload", filePath, "", 0, err)
					writeTransferFailure(s.Conn, "Upload", err)
					return false
				}
				s.endTransfer()

				if fileSize == 0 && zipscript.ShouldDeleteZeroByteForDir(s.Config.Zipscript, uploadDir) {
					bridge.DeleteFile(filePath)
					log.Printf("[MASTER-ZS] Deleted 0-byte file: %s", filePath)
					fmt.Fprintf(s.Conn, "226 Transfer complete.\r\n")
					return false
				}
				if !strings.HasSuffix(strings.ToLower(fileName), ".sfv") {
					sfvEntries := bridge.GetSFVData(uploadDir)
					if expectedCRC, exists := cachedExpectedCRC(sfvEntries, fileName); exists {
						writeUploadSFVStatus(s.Conn, checksum, expectedCRC, true, fileSize)
					} else {
						writeUploadNoSFVEntryStatus(s.Conn, sfvEntries, fileName)
					}
				}

				mediaInfoDir := storReleaseMediaDir(uploadDir, filePath)

				transferredBytes := fileSize
				if restOffset > 0 && fileSize > restOffset {
					transferredBytes = fileSize - restOffset
				}
				speedMB := 0.0
				if xferMs > 0 {
					speedMB = (float64(transferredBytes) / 1024.0 / 1024.0) / (float64(xferMs) / 1000.0)
				}
				fmt.Fprintf(s.Conn, "226 Transfer complete.\r\n")
				queueMasterUploadPostHooks(s, bridge, uploadDir, mediaInfoDir, filePath, fileName, checksum, transferredBytes, fileSize, speedMB, xferMs, existingNames)
				return false
			}
		}

		raw, err := s.getRawDataConn()
		if err != nil {
			if s.PassthruSlave != nil && s.Config.Passthrough {
			} else {
				fmt.Fprintf(s.Conn, "425 Data connection failed\r\n")
				return false
			}
		}

		if s.Config.Mode == "master" && s.MasterManager != nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				filePath := uploadPath

				var fileSize int64
				var checksum uint32
				var xferMs int64

				if s.PassthruSlave != nil && s.Config.Passthrough {
					slaveName := s.PassthruSlave.(string)
					fmt.Fprintf(s.Conn, "150 Opening %s mode data connection.\r\n", transferTypeReplyName(s.TransferType))
					log.Printf("[Passthrough] STOR %s via slave %s (xferIdx=%d)", filePath, slaveName, s.PassthruXferIdx)
					s.beginTransferOnSlave("upload", filePath, slaveName, s.PassthruXferIdx)
					defer s.endTransfer()

					fileSize, checksum, xferMs, err = bridge.SlaveReceivePassthrough(filePath, s.PassthruXferIdx, slaveName, s.User.Name, s.User.PrimaryGroup, restOffset, s.currentTransferTypeByte())
					s.PassthruSlave = nil
					s.PretCmd = ""
					s.PretArg = ""

					if err != nil {
						if writeDuplicateUploadResponse(s, bridge, uploadDir, fileName, err) {
							return false
						}
						if s.Config != nil && s.Config.Debug {
							log.Printf("[Passthrough] Upload failed for user %s path %s: %s", s.User.Name, filePath, formatTransferFailureLog(err))
						}
						maybeHandleSlowTransfer(s, "upload", filePath, slaveName, s.PassthruXferIdx, err)
						writeTransferFailure(s.Conn, "Upload", err)
						return false
					}
				} else {
					fmt.Fprintf(s.Conn, "150 Opening %s mode data connection.\r\n", transferTypeReplyName(s.TransferType))
					dataConn, err := s.upgradeDataTLS(raw, tlsConfig)
					if err != nil {
						raw.Close()
						return false
					}
					s.beginTransfer("upload", filePath)
					defer s.endTransfer()
					dataConn = trackTransferConn(s, dataConn, "upload")

					start := time.Now()
					fileSize, checksum, err = bridge.UploadFile(filePath, dataConn, s.User.Name, s.User.PrimaryGroup, restOffset, s.currentTransferTypeByte())
					xferMs = time.Since(start).Milliseconds()
					dataConn.Close()

					if err != nil {
						log.Printf("[MASTER] Upload failed: %v", err)
						maybeHandleSlowTransfer(s, "upload", filePath, "", 0, err)
						writeTransferFailure(s.Conn, "Upload", err)
						return false
					}
				}
				s.endTransfer()

				if fileSize == 0 && zipscript.ShouldDeleteZeroByteForDir(s.Config.Zipscript, uploadDir) {
					bridge.DeleteFile(filePath)
					log.Printf("[MASTER-ZS] Deleted 0-byte file: %s", filePath)
					fmt.Fprintf(s.Conn, "226 Transfer complete.\r\n")
					return false
				}
				if !strings.HasSuffix(strings.ToLower(fileName), ".sfv") {
					sfvEntries := bridge.GetSFVData(uploadDir)
					if expectedCRC, exists := cachedExpectedCRC(sfvEntries, fileName); exists {
						writeUploadSFVStatus(s.Conn, checksum, expectedCRC, true, fileSize)
					} else {
						writeUploadNoSFVEntryStatus(s.Conn, sfvEntries, fileName)
					}
				}

				mediaInfoDir := storReleaseMediaDir(uploadDir, filePath)

				transferredBytes := fileSize
				if restOffset > 0 && fileSize > restOffset {
					transferredBytes = fileSize - restOffset
				}
				speedMB := 0.0
				if xferMs > 0 {
					speedMB = (float64(transferredBytes) / 1024.0 / 1024.0) / (float64(xferMs) / 1000.0)
				}
				fmt.Fprintf(s.Conn, "226 Transfer complete.\r\n")
				queueMasterUploadPostHooks(s, bridge, uploadDir, mediaInfoDir, filePath, fileName, checksum, transferredBytes, fileSize, speedMB, xferMs, existingNames)
			} else {
				fmt.Fprintf(s.Conn, "550 Master not initialized\r\n")
				if raw != nil {
					raw.Close()
				}
			}
			return false
		}

		if restOffset > 0 {
			info, err := os.Stat(localPath)
			if err != nil {
				fmt.Fprintf(s.Conn, "550 Resume target not found.\r\n")
				return false
			}
			if restOffset > info.Size() {
				fmt.Fprintf(s.Conn, "550 Resume offset beyond end of file.\r\n")
				return false
			}
		} else if info, err := os.Stat(localPath); err == nil && !info.IsDir() {
			if s.Config.XdupeEnabled {
				dirEntries, readErr := os.ReadDir(filepath.Dir(localPath))
				if readErr == nil {
					var names []string
					for _, entry := range dirEntries {
						names = append(names, entry.Name())
					}
					for _, line := range xdupeResponseLines(s.XDupeMode, names) {
						fmt.Fprintf(s.Conn, "553-%s\r\n", line)
					}
				}
				fmt.Fprintf(s.Conn, "553 %s: file already exists (X-DUPE)\r\n", fileName)
			} else {
				fmt.Fprintf(s.Conn, "553 %s: file already exists\r\n", fileName)
			}
			return false
		}

		if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
			if raw != nil {
				raw.Close()
			}
			fmt.Fprintf(s.Conn, "550 Upload prepare failed: %v\r\n", err)
			return false
		}

		fmt.Fprintf(s.Conn, "150 Opening %s mode data connection.\r\n", transferTypeReplyName(s.TransferType))
		dataConn, err := s.upgradeDataTLS(raw, tlsConfig)
		if err != nil {
			raw.Close()
			return false
		}

		flags := os.O_CREATE | os.O_WRONLY
		if restOffset > 0 {
			flags |= os.O_APPEND
		} else {
			flags |= os.O_TRUNC
		}
		file, err := os.OpenFile(localPath, flags, 0644)
		if err != nil {
			dataConn.Close()
			writeTransferFailure(s.Conn, "Upload", err)
			return false
		}
		if restOffset > 0 {
			if _, err := file.Seek(restOffset, io.SeekStart); err != nil {
				file.Close()
				dataConn.Close()
				writeTransferFailure(s.Conn, "Upload", err)
				return false
			}
		}

		s.beginTransfer("upload", uploadPath)
		defer s.endTransfer()
		dataConn = trackTransferConn(s, dataConn, "upload")

		start := time.Now()
		var checksum uint32
		writer := io.Writer(file)
		var checksumHash hash.Hash32
		if restOffset == 0 {
			checksumHash = crc32.NewIEEE()
			writer = io.MultiWriter(file, checksumHash)
		}
		written, err := io.Copy(writer, dataConn)
		xferMs := time.Since(start).Milliseconds()
		file.Close()
		dataConn.Close()
		if err != nil {
			if restOffset == 0 {
				_ = os.Remove(localPath)
			}
			writeTransferFailure(s.Conn, "Upload", err)
			return false
		}
		s.endTransfer()
		if checksumHash != nil {
			checksum = checksumHash.Sum32()
		}
		fileSize := written
		if restOffset > 0 {
			fileSize += restOffset
		}
		if fileSize == 0 && zipscript.ShouldDeleteZeroByteForDir(s.Config.Zipscript, uploadDir) {
			_ = os.Remove(localPath)
			fmt.Fprintf(s.Conn, "226 Transfer complete.\r\n")
			return false
		}
		if badZip, err := localCheckUploadedZipIntegrity(s.Config, uploadDir, localPath, fileName); err != nil && s.Config.Debug {
			log.Printf("[LOCAL-ZS] zip integrity check skipped for %s: %v", uploadPath, err)
		} else if badZip {
			fmt.Fprintf(s.Conn, "226 Zip integrity check failed, deleting file\r\n")
			return false
		}
		if checksum > 0 && zipscript.ShouldDeleteBadCRCForDir(s.Config.Zipscript, uploadDir) && !strings.HasSuffix(strings.ToLower(fileName), ".sfv") {
			if expectedCRC, ok := localExpectedCRCForFile(localPath); ok && expectedCRC != checksum {
				_ = os.Remove(localPath)
				createLocalSFVMissingMarker(s.Config, filepath.Dir(localPath), fileName)
				fmt.Fprintf(s.Conn, "226- checksum mismatch: SLAVE: %08X SFV: %08X\r\n", checksum, expectedCRC)
				fmt.Fprintf(s.Conn, "226 Checksum mismatch, deleting file\r\n")
				return false
			}
		}
		if !strings.HasSuffix(strings.ToLower(fileName), ".sfv") {
			sfvEntries := localSFVEntriesForDir(filepath.Dir(localPath))
			if expectedCRC, ok := cachedExpectedCRC(sfvEntries, fileName); ok {
				writeUploadSFVStatus(s.Conn, checksum, expectedCRC, true, fileSize)
				if checksum == expectedCRC && checksum != 0 {
					clearLocalSFVMissingMarker(filepath.Dir(localPath), fileName)
				}
			} else {
				writeUploadNoSFVEntryStatus(s.Conn, sfvEntries, fileName)
			}
		} else {
			syncLocalSFVMissingMarkers(s.Config, filepath.Dir(localPath))
		}

		isSpeedtest := isSpeedtestPath(uploadPath)
		transferredBytes := written
		if transferredBytes > 0 {
			s.User.UpdateStatsWithCredits(transferredBytes, true, !isSpeedtest)
		}
		speedMB := transferSpeedMB(transferredBytes, xferMs)
		fmt.Fprintf(s.Conn, "226 Transfer complete.\r\n")
		go func(uploadPath, fileName, localPath string, transferredBytes int64, speedMB float64) {
			s.emitEvent(EventUpload, uploadPath, fileName, transferredBytes, speedMB, nil)
			if err := localRefreshZipDIZFromArchive(filepath.Dir(localPath), localPath, fileName); err != nil && s.Config.Debug {
				log.Printf("[LOCAL-ZS] zip diz refresh skipped for %s: %v", uploadPath, err)
			}
		}(uploadPath, fileName, localPath, transferredBytes, speedMB)
		return false

	case "RETR":
		if len(args) == 0 {
			return false
		}
		defer s.clearPreparedTransferState()
		if !s.hasPreparedTransferChannel() {
			fmt.Fprintf(s.Conn, "503 Bad sequence of commands.\r\n")
			return false
		}

		isTLSExempt := false
		for _, exemptUser := range s.Config.TLSExemptUsers {
			if exemptUser == s.User.Name {
				isTLSExempt = true
				break
			}
		}
		if s.Config.RequireTLSData && !isTLSExempt && !s.DataTLS {
			fmt.Fprintf(s.Conn, "550 TLS required for data transfers.\r\n")
			return false
		}

		filePath := path.Clean(path.Join(s.CurrentDir, args[0]))
		if path.IsAbs(strings.TrimSpace(args[0])) {
			filePath = path.Clean(args[0])
		}
		if s.Config.Mode == "master" && s.MasterManager != nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				filePath = bridge.ResolvePath(filePath)
			}
		}
		aclPath := path.Join(s.Config.ACLBasePath, filePath)
		if !s.ACLEngine.CanPerform(s.User, "DOWNLOAD", aclPath) {
			fmt.Fprintf(s.Conn, "550 Access Denied.\r\n")
			return false
		}
		restOffset := s.RestOffset
		s.RestOffset = 0

		if s.Config.Mode == "master" && s.MasterManager != nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				if restOffset > 0 && !zipscript.AllowResumeForDir(s.Config.Zipscript, path.Dir(filePath)) {
					fmt.Fprintf(s.Conn, "550 Resume is disabled for this release.\r\n")
					return false
				}
				fileSize := bridge.GetFileSize(filePath)
				if fileSize < 0 {
					fmt.Fprintf(s.Conn, "550 File not found on any slave.\r\n")
					return false
				}
				if activeUploadForPathWithBridge(bridge, filePath) {
					fmt.Fprintf(s.Conn, "550 No Permission To Download A File Currently Being Uploaded.\r\n")
					return false
				}
				if shouldTreatDownloadAsMissing(s.Config, bridge, filePath) {
					fmt.Fprintf(s.Conn, "550 File is incomplete or failed checksum verification.\r\n")
					return false
				}
				if restOffset > fileSize {
					fmt.Fprintf(s.Conn, "550 Resume offset beyond end of file.\r\n")
					return false
				}
				isSpeedtest := isSpeedtestPath(filePath)
				remainingSize := fileSize
				if restOffset > 0 && restOffset < fileSize {
					remainingSize = fileSize - restOffset
				}
				if !isSpeedtest && !s.User.HasDownloadAccess() {
					fmt.Fprintf(s.Conn, "550 No permission to download.\r\n")
					return false
				}
				if !isSpeedtest && !s.User.HasEnoughCredits(remainingSize) {
					fmt.Fprintf(s.Conn, "550 Not enough credits.\r\n")
					return false
				}
				if s.User != nil && s.User.DownloadSlots > 0 {
					activeDownloads := countTransfersForUser(s.User.Name, "download")
					if activeDownloads >= s.User.DownloadSlots {
						fmt.Fprintf(s.Conn, "550 Maximum simultaneous downloads reached (%d).\r\n", s.User.DownloadSlots)
						return false
					}
				}
				if s.User != nil && s.User.MaxSim > 0 {
					activeTransfers := countTransfersForUserAllDirections(s.User.Name)
					if activeTransfers >= s.User.MaxSim {
						fmt.Fprintf(s.Conn, "550 Maximum simultaneous transfers reached (%d).\r\n", s.User.MaxSim)
						return false
					}
				}
				if s.Config.Passthrough && s.ActiveAddr != "" && s.PassthruSlave == nil {
					portAddr := s.ActiveAddr
					s.ActiveAddr = ""
					fmt.Fprintf(s.Conn, "150 Opening %s mode data connection for %s (%d bytes).\r\n", transferTypeReplyName(s.TransferType), args[0], fileSize)
					log.Printf("[Passthrough] PORT RETR %s by %s -> %s", filePath, s.User.Name, portAddr)
					s.beginTransfer("download", filePath)
					defer s.endTransfer()

					transferChecksum, xferMs, err := bridge.SlaveConnectAndSend(filePath, portAddr, s.User.Name, s.User.PrimaryGroup, restOffset, s.DataTLS, s.SSCN, s.currentTransferTypeByte())
					if err != nil {
						if s.Config != nil && s.Config.Debug {
							log.Printf("[Passthrough] PORT download failed for user %s path %s: %s", s.User.Name, filePath, formatTransferFailureLog(err))
						}
						maybeHandleSlowTransfer(s, "download", filePath, "", 0, err)
						writeTransferFailure(s.Conn, "Download", err)
						return false
					}

					if restOffset == 0 && transferChecksum != 0 {
						if expectedCRC, ok := cachedExpectedCRC(bridge.GetSFVData(path.Dir(filePath)), path.Base(filePath)); ok && transferChecksum != expectedCRC {
							fmt.Fprintf(s.Conn, "226- WARNING: checksum from transfer didn't match checksum in .sfv\r\n")
						}
					}
					fmt.Fprintf(s.Conn, "226 Transfer complete.\r\n")
					if remainingSize > 0 {
						s.User.UpdateStatsWithCredits(remainingSize, false, !isSpeedtest)
					}
					s.emitEvent(EventDownload, filePath, args[0], remainingSize, transferSpeedMB(remainingSize, xferMs), nil)
					return false
				}

				if s.PassthruSlave != nil && s.Config.Passthrough {
					slaveName := s.PassthruSlave.(string)
					fmt.Fprintf(s.Conn, "150 Opening %s mode data connection for %s (%d bytes).\r\n", transferTypeReplyName(s.TransferType), args[0], fileSize)
					log.Printf("[Passthrough] RETR %s by %s via slave %s (xferIdx=%d)", filePath, s.User.Name, slaveName, s.PassthruXferIdx)
					s.beginTransferOnSlave("download", filePath, slaveName, s.PassthruXferIdx)
					defer s.endTransfer()

					start := time.Now()
					transferChecksum, xferMs, err := bridge.SlaveSendPassthrough(filePath, s.PassthruXferIdx, slaveName, s.User.Name, s.User.PrimaryGroup, restOffset, s.currentTransferTypeByte())
					if xferMs == 0 {
						xferMs = time.Since(start).Milliseconds()
					}
					s.PassthruSlave = nil
					s.PretCmd = ""
					s.PretArg = ""

					if err != nil {
						if s.Config != nil && s.Config.Debug {
							log.Printf("[Passthrough] Download failed for user %s path %s: %s", s.User.Name, filePath, formatTransferFailureLog(err))
						}
						maybeHandleSlowTransfer(s, "download", filePath, slaveName, s.PassthruXferIdx, err)
						writeTransferFailure(s.Conn, "Download", err)
					} else {
						if restOffset == 0 && transferChecksum != 0 {
							if expectedCRC, ok := cachedExpectedCRC(bridge.GetSFVData(path.Dir(filePath)), path.Base(filePath)); ok && transferChecksum != expectedCRC {
								fmt.Fprintf(s.Conn, "226- WARNING: checksum from transfer didn't match checksum in .sfv\r\n")
							}
						}
						fmt.Fprintf(s.Conn, "226 Transfer complete.\r\n")
						if remainingSize > 0 {
							s.User.UpdateStatsWithCredits(remainingSize, false, !isSpeedtest)
						}
						s.emitEvent(EventDownload, filePath, args[0], remainingSize, transferSpeedMB(remainingSize, xferMs), nil)
					}
				} else {
					raw, err := s.getRawDataConn()
					if err != nil {
						fmt.Fprintf(s.Conn, "425 Data connection failed\r\n")
						return false
					}
					fmt.Fprintf(s.Conn, "150 Opening %s mode data connection for %s (%d bytes).\r\n", transferTypeReplyName(s.TransferType), args[0], fileSize)
					dataConn, err := s.upgradeDataTLS(raw, tlsConfig)
					if err != nil {
						raw.Close()
						return false
					}
					s.beginTransfer("download", filePath)
					defer s.endTransfer()
					dataConn = trackTransferConn(s, dataConn, "download")
					start := time.Now()
					transferChecksum, err := bridge.DownloadFile(filePath, dataConn, s.User.Name, s.User.PrimaryGroup, restOffset, s.currentTransferTypeByte())
					xferMs := time.Since(start).Milliseconds()
					dataConn.Close()
					s.PretCmd = ""
					s.PretArg = ""
					if err != nil {
						log.Printf("[MASTER] Download failed: %v", err)
						maybeHandleSlowTransfer(s, "download", filePath, "", 0, err)
						writeTransferFailure(s.Conn, "Download", err)
					} else {
						if restOffset == 0 && transferChecksum != 0 {
							if expectedCRC, ok := cachedExpectedCRC(bridge.GetSFVData(path.Dir(filePath)), path.Base(filePath)); ok && transferChecksum != expectedCRC {
								fmt.Fprintf(s.Conn, "226- WARNING: checksum from transfer didn't match checksum in .sfv\r\n")
							}
						}
						fmt.Fprintf(s.Conn, "226 Transfer complete.\r\n")
						if remainingSize > 0 {
							s.User.UpdateStatsWithCredits(remainingSize, false, !isSpeedtest)
						}
						s.emitEvent(EventDownload, filePath, args[0], remainingSize, transferSpeedMB(remainingSize, xferMs), nil)
					}
				}
			} else {
				fmt.Fprintf(s.Conn, "550 Master not initialized\r\n")
			}
			return false
		}

		localPath := filepath.Join(s.Config.StoragePath, filepath.FromSlash(strings.TrimPrefix(filePath, "/")))
		if restOffset > 0 && !zipscript.AllowResumeForDir(s.Config.Zipscript, path.Dir(filePath)) {
			fmt.Fprintf(s.Conn, "550 Resume is disabled for this release.\r\n")
			return false
		}
		info, err := os.Stat(localPath)
		if err != nil {
			fmt.Fprintf(s.Conn, "550 File not found.\r\n")
			return false
		}
		if info.IsDir() {
			fmt.Fprintf(s.Conn, "550 Requested target is not a file.\r\n")
			return false
		}
		fileSize := info.Size()
		if activeUploadForPath(filePath) {
			fmt.Fprintf(s.Conn, "550 No Permission To Download A File Currently Being Uploaded.\r\n")
			return false
		}
		if localShouldTreatDownloadAsMissing(s.Config, filePath, localPath) {
			fmt.Fprintf(s.Conn, "550 File is incomplete or failed checksum verification.\r\n")
			return false
		}
		if restOffset > fileSize {
			fmt.Fprintf(s.Conn, "550 Resume offset beyond end of file.\r\n")
			return false
		}
		isSpeedtest := isSpeedtestPath(filePath)
		remainingSize := fileSize
		if restOffset > 0 && restOffset < fileSize {
			remainingSize = fileSize - restOffset
		}
		if !isSpeedtest && !s.User.HasDownloadAccess() {
			fmt.Fprintf(s.Conn, "550 No permission to download.\r\n")
			return false
		}
		if !isSpeedtest && !s.User.HasEnoughCredits(remainingSize) {
			fmt.Fprintf(s.Conn, "550 Not enough credits.\r\n")
			return false
		}
		if s.User != nil && s.User.DownloadSlots > 0 {
			activeDownloads := countTransfersForUser(s.User.Name, "download")
			if activeDownloads >= s.User.DownloadSlots {
				fmt.Fprintf(s.Conn, "550 Maximum simultaneous downloads reached (%d).\r\n", s.User.DownloadSlots)
				return false
			}
		}
		if s.User != nil && s.User.MaxSim > 0 {
			activeTransfers := countTransfersForUserAllDirections(s.User.Name)
			if activeTransfers >= s.User.MaxSim {
				fmt.Fprintf(s.Conn, "550 Maximum simultaneous transfers reached (%d).\r\n", s.User.MaxSim)
				return false
			}
		}
		raw, err := s.getRawDataConn()
		if err != nil {
			fmt.Fprintf(s.Conn, "425 Data connection failed\r\n")
			return false
		}
		fmt.Fprintf(s.Conn, "150 Opening %s mode data connection for %s (%d bytes).\r\n", transferTypeReplyName(s.TransferType), args[0], fileSize)
		dataConn, err := s.upgradeDataTLS(raw, tlsConfig)
		if err != nil {
			raw.Close()
			return false
		}

		file, err := os.Open(localPath)
		if err != nil {
			dataConn.Close()
			writeTransferFailure(s.Conn, "Download", err)
			return false
		}
		defer file.Close()
		if restOffset > 0 {
			if _, err := file.Seek(restOffset, io.SeekStart); err != nil {
				dataConn.Close()
				writeTransferFailure(s.Conn, "Download", err)
				return false
			}
		}

		s.beginTransfer("download", filePath)
		defer s.endTransfer()
		dataConn = trackTransferConn(s, dataConn, "download")

		start := time.Now()
		_, err = io.Copy(dataConn, file)
		xferMs := time.Since(start).Milliseconds()
		dataConn.Close()
		if err != nil {
			writeTransferFailure(s.Conn, "Download", err)
			return false
		}
		fmt.Fprintf(s.Conn, "226 Transfer complete.\r\n")
		if remainingSize > 0 {
			s.User.UpdateStatsWithCredits(remainingSize, false, !isSpeedtest)
		}
		s.emitEvent(EventDownload, filePath, args[0], remainingSize, transferSpeedMB(remainingSize, xferMs), nil)
		return false

	case "STAT":
		// STAT with no args = server status. STAT <path> = listing on control
		// channel (no data connection). cbftp uses STAT -l at login as a
		// cheap way to probe the server without opening a data conn.
		if len(args) == 0 {
			typeName := "ASCII"
			if strings.EqualFold(s.TransferType, "I") {
				typeName = "BINARY"
			}
			fmt.Fprintf(s.Conn, "211- %s server status:\r\n", s.Config.SiteNameShort)
			fmt.Fprintf(s.Conn, " Connected from %s\r\n", s.Conn.RemoteAddr())
			fmt.Fprintf(s.Conn, " Logged in as %s\r\n", s.User.Name)
			fmt.Fprintf(s.Conn, " TYPE: %s, STRU: F, MODE: S\r\n", typeName)
			fmt.Fprintf(s.Conn, "211 End of status.\r\n")
			return false
		}

		target := s.resolveListTargetPath("LIST", args, s.masterBridgeOrNil())
		if err := s.validateListDirectoryTarget(target, s.masterBridgeOrNil()); err != nil {
			if err.Error() == "504" {
				fmt.Fprintf(s.Conn, "504 Command not implemented for that parameter.\r\n")
			} else {
				fmt.Fprintf(s.Conn, "550 %s: no such file or directory\r\n", target)
			}
			return false
		}

		fmt.Fprintf(s.Conn, "213-STAT\r\n")
		fmt.Fprintf(s.Conn, " total 0\r\n")
		if s.Config.Mode == "master" && s.MasterManager != nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				entries := bridge.ListDir(target)
				now := timeutil.Now().Format("Jan _2 15:04")
				siteName := s.Config.SiteNameShort
				if siteName == "" {
					siteName = "GoFTPd"
				}

				if zipscript.ShowStatusBarForDir(s.Config.Zipscript, target) {
					if statusName := dirRaceStatusName(bridge, s.Config, target, siteName); strings.TrimSpace(statusName) != "" {
						mode := "drwxr-xr-x"
						size := "4096"
						if !zipscript.StatusBarDirectoryForDir(s.Config.Zipscript, target) {
							mode = "-rw-r--r--"
							size = "0"
						}
						fmt.Fprintf(s.Conn, " %s   1 %-8s %-8s %10s %s %s\r\n",
							mode, "GoFTPd", "GoFTPd", size, now, statusName)
					}
				}

				for _, e := range entries {
					if strings.HasPrefix(e.Name, ".") {
						continue
					}
					if strings.HasPrefix(e.Name, "[#") || strings.HasPrefix(e.Name, "[:") {
						continue
					}
					if strings.Contains(e.Name, "COMPLETE") && strings.Contains(e.Name, "[") {
						continue
					}
					aclPath := path.Join(s.Config.ACLBasePath, target, e.Name)
					if !s.ACLEngine.CanPerform(s.User, "LIST", aclPath) {
						continue
					}

					mode := ftpListMode(e)
					size := fmt.Sprintf("%d", e.Size)
					name := e.Name
					if e.IsSymlink {
						size = "0"
						name = fmt.Sprintf("%s -> %s", e.Name, e.LinkTarget)
					} else if e.IsDir {
						size = "4096"
					}
					ts := timeutil.Unix(e.ModTime).Format("Jan _2 15:04")
					owner := "GoFTPd"
					group := "GoFTPd"
					if s.Config.ShowRealOwnerGroup {
						owner = strings.TrimSpace(e.Owner)
						group = strings.TrimSpace(e.Group)
						if owner == "" {
							owner = "GoFTPd"
						}
						if group == "" {
							group = "GoFTPd"
						}
					}
					fmt.Fprintf(s.Conn, " %s   1 %-8s %-8s %10s %s %s\r\n",
						mode, owner, group, size, ts, name)
				}

			}
		} else {
			listPath := filepath.Join(s.Config.StoragePath, target)
			files, err := os.ReadDir(listPath)
			if err == nil {
				for _, f := range files {
					if strings.HasPrefix(f.Name(), ".") {
						continue
					}
					if !s.Config.ShowSymlinks && f.Type()&fs.ModeSymlink != 0 {
						continue
					}
					if !s.canListPath(path.Join(target, f.Name())) {
						continue
					}
					info, err := f.Info()
					if err != nil {
						continue
					}
					mode := "-rw-r--r--"
					size := fmt.Sprintf("%d", info.Size())
					if info.IsDir() {
						mode = "drwxr-xr-x"
						size = "4096"
					} else if f.Type()&fs.ModeSymlink != 0 {
						mode = "lrwxrwxrwx"
					}
					ts := timeutil.In(info.ModTime()).Format("Jan _2 15:04")
					fmt.Fprintf(s.Conn, " %s   1 %-8s %-8s %10s %s %s\r\n",
						mode, "GoFTPd", "GoFTPd", size, ts, f.Name())
				}
			}
		}
		fmt.Fprintf(s.Conn, "213 End of status.\r\n")
		return false

	case "QUIT":
		fmt.Fprintf(s.Conn, "221 Goodbye.\r\n")
		return true

	default:
		fmt.Fprintf(s.Conn, "502 Command not implemented.\r\n")
	}
	return false
}

func (s *Session) showGlobalStats(code string, final bool) {
	freeSpaceMB := uint64(0)
	if s.Config.Mode == "master" && s.MasterManager != nil {
		if bridge, ok := s.MasterManager.(MasterBridge); ok {
			if freeBytes, _, ok := bridge.GetAggregateDiskUsage(); ok && freeBytes > 0 {
				freeSpaceMB = uint64(freeBytes) / 1024 / 1024
			}
		}
	}
	if freeSpaceMB == 0 {
		var stat syscall.Statfs_t
		wd, _ := os.Getwd()
		if err := syscall.Statfs(s.Config.StoragePath, &stat); err != nil {
			_ = syscall.Statfs(wd, &stat)
		}
		freeSpaceMB = (stat.Bavail * uint64(stat.Bsize)) / 1024 / 1024
	}
	siteSpeedMiB := 0.0

	ulGiB := 0.0
	dlGiB := 0.0
	creditsGiB := 0.0
	ratioStr := "UL&DL: Unlimited"

	if s.User != nil {
		ulGiB = float64(s.User.AllUp.Bytes) / (1024 * 1024 * 1024)
		dlGiB = float64(s.User.AllDn.Bytes) / (1024 * 1024 * 1024)
		creditsGiB = float64(s.User.Credits) / (1024 * 1024 * 1024)
		if s.User.Ratio > 0 {
			ratioStr = fmt.Sprintf("1:%d", s.User.Ratio)
		}
	}

	fmt.Fprintf(s.Conn, "%s- [Ul: %.1fGiB] [Dl: %.1fGiB] [Speed: %.2fMiB/s] [Free: %dMB]\r\n",
		code, ulGiB, dlGiB, siteSpeedMiB, freeSpaceMB)

	if final {
		fmt.Fprintf(s.Conn, "%s  [Credits: %.1fGiB] [Ratio: %s]\r\n",
			code, creditsGiB, ratioStr)
	} else {
		fmt.Fprintf(s.Conn, "%s- [Credits: %.1fGiB] [Ratio: %s]\r\n",
			code, creditsGiB, ratioStr)
	}
}

func (s *Session) persistLoginStateAsync(username string, lastLogin int64) {
	username = strings.TrimSpace(username)
	if username == "" || lastLogin <= 0 {
		return
	}
	groupMap := s.GroupMap
	debug := s.Config != nil && s.Config.Debug
	go func() {
		_, err := user.MutateAndSave(username, groupMap, func(current *user.User) error {
			current.LastLogin = lastLogin
			current.ResetTransferStatPeriodsIfDue(time.Unix(lastLogin, 0))
			return nil
		})
		if err != nil && debug {
			log.Printf("[USER] skipped login-state save for %s: %v", username, err)
		}
	}()
}

func mbString(size int64) string { return fmt.Sprintf("%.0fMB", float64(size)/1024.0/1024.0) }

func isSpeedtestPath(p string) bool {
	clean := strings.ToLower(path.Clean("/" + strings.TrimSpace(p)))
	return clean == "/speedtest" || strings.HasPrefix(clean, "/speedtest/")
}

func transferSpeedMB(size int64, xferMs int64) float64 {
	if size <= 0 || xferMs <= 0 {
		return 0
	}
	return (float64(size) / 1024.0 / 1024.0) / (float64(xferMs) / 1000.0)
}

func ftpListMode(e MasterFileEntry) string {
	switch {
	case e.IsSymlink:
		return "lrwxrwxrwx"
	case e.IsDir:
		if e.Mode == 0555 {
			return "dr-xr-xr-x"
		}
		return "drwxr-xr-x"
	default:
		if e.Mode == 0444 {
			return "-r--r--r--"
		}
		return "-rw-r--r--"
	}
}

func mlsdSymlinkType(e MasterFileEntry) string {
	target := strings.TrimSpace(e.LinkTarget)
	if target == "" {
		return "OS.unix=symlink"
	}
	return "OS.unix=slink:" + target
}

func incompleteMarkerName(pattern, relname string) string {
	return zipscript.StatusMarkerName(pattern, relname)
}

func incompleteMarkerName2(pattern, relname, child string) string {
	return zipscript.StatusMarkerNameForChild(pattern, relname, child)
}

func markerLinkTarget(dirPath, relName string) string {
	return path.Clean(path.Join("/", strings.TrimSpace(dirPath), strings.TrimSpace(relName)))
}

func isIncompleteMarkerName(pattern, name string) bool {
	if strings.TrimSpace(pattern) == "" {
		return strings.HasPrefix(strings.ToLower(name), "[incomplete]")
	}
	return zipscript.IsStatusMarkerName(zipscript.Config{
		Enabled: true,
		Incomplete: zipscript.IncompleteConfig{
			Enabled:        true,
			Indicator:      pattern,
			NoSFVIndicator: pattern,
			NFOIndicator:   pattern,
			CDIndicator:    pattern,
		},
	}, name)
}

func resolveKnownMarkerTarget(bridge MasterBridge, cfg *Config, parent, name string) string {
	if bridge == nil || cfg == nil {
		return ""
	}
	patterns := []string{
		activeIncompleteIndicator(cfg),
		zipscript.NoSFVIndicator(cfg.Zipscript),
		zipscript.NFOIndicator(cfg.Zipscript),
		zipscript.CDIndicator(cfg.Zipscript),
	}
	seen := make(map[string]struct{}, len(patterns))
	for _, pattern := range patterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		if _, ok := seen[pattern]; ok {
			continue
		}
		seen[pattern] = struct{}{}
		if !isIncompleteMarkerName(pattern, name) {
			continue
		}
		markerPath := path.Clean(path.Join("/", strings.TrimSpace(parent), strings.TrimSpace(name)))
		if entry, found := bridge.GetPathEntry(markerPath); found && entry.IsSymlink && strings.TrimSpace(entry.LinkTarget) != "" {
			target := path.Clean("/" + strings.TrimSpace(entry.LinkTarget))
			if bridge.FileExists(target) {
				return target
			}
			return target
		}
	}
	return ""
}

func hasNFOEntry(entries []MasterFileEntry) bool {
	for _, entry := range entries {
		if entry.IsDir {
			continue
		}
		if strings.HasSuffix(strings.ToLower(entry.Name), ".nfo") {
			return true
		}
	}
	return false
}

func hasSFVEntry(entries []MasterFileEntry) bool {
	for _, entry := range entries {
		if entry.IsDir {
			continue
		}
		if strings.HasSuffix(strings.ToLower(entry.Name), ".sfv") {
			return true
		}
	}
	return false
}

func progressBar(present, total, width int) string {
	if total <= 0 {
		total = 1
	}
	if width <= 0 {
		width = 20
	}
	filled := (present * width) / total
	if filled < 0 {
		filled = 0
	}
	if filled > width {
		filled = width
	}
	var b strings.Builder
	b.WriteByte('[')
	for i := 0; i < width; i++ {
		if i < filled {
			b.WriteByte('#')
		} else {
			b.WriteByte(':')
		}
	}
	b.WriteByte(']')
	return b.String()
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func currentRaceSpeedMB(dirPath string, totalBytes int64, bridge MasterBridge) float64 {
	if bridge == nil || totalBytes <= 0 {
		return 0
	}
	ms := bridge.GetRaceWallClockMilliseconds(dirPath)
	if ms <= 0 {
		return 0
	}
	return (float64(totalBytes) / 1024.0 / 1024.0) / (float64(ms) / 1000.0)
}

func aggregateRaceSpeedMB(users []VFSRaceUser) float64 {
	total := 0.0
	for _, u := range users {
		if u.Speed > 0 {
			total += u.Speed
		}
	}
	return total / 1024.0 / 1024.0
}

func maxUserRaceDurationMs(users []VFSRaceUser) int64 {
	var maxMs int64
	for _, u := range users {
		if u.DurationMs > maxMs {
			maxMs = u.DurationMs
		}
	}
	return maxMs
}

func raceSpeedMBForDuration(totalBytes int64, durationMs int64) float64 {
	if totalBytes <= 0 || durationMs <= 0 {
		return 0
	}
	return (float64(totalBytes) / 1024.0 / 1024.0) / (float64(durationMs) / 1000.0)
}

func estimateRaceTimeLeftWithSpeed(totalBytes int64, present, total int, speedMB float64) string {
	if totalBytes <= 0 || present <= 0 || total <= present {
		return "0s"
	}
	if speedMB <= 0 {
		return "N/A"
	}
	avgBytesPerFile := float64(totalBytes) / float64(present)
	bytesLeft := avgBytesPerFile * float64(total-present)
	seconds := int((bytesLeft / 1024.0 / 1024.0) / speedMB)
	if seconds < 1 {
		seconds = 1
	}
	return fmt.Sprintf("%ds", seconds)
}

func estimateRaceTimeLeft(dirPath string, totalBytes int64, present, total int, bridge MasterBridge) string {
	if totalBytes <= 0 || present <= 0 || total <= present {
		return "0s"
	}
	speed := currentRaceSpeedMB(dirPath, totalBytes, bridge)
	if speed <= 0 {
		return "N/A"
	}
	avgBytesPerFile := float64(totalBytes) / float64(present)
	bytesLeft := avgBytesPerFile * float64(total-present)
	seconds := int((bytesLeft / 1024.0 / 1024.0) / speed)
	if seconds < 1 {
		seconds = 1
	}
	return fmt.Sprintf("%ds", seconds)
}

func estimateZipTimeLeft(dirPath string, totalBytes int64, present, total int, bridge MasterBridge) string {
	if totalBytes <= 0 || present <= 0 || total <= present {
		return "0s"
	}
	speed := currentRaceSpeedMB(dirPath, totalBytes, bridge)
	if speed <= 0 {
		return "N/A"
	}
	avgBytesPerFile := float64(totalBytes) / float64(present)
	bytesLeft := avgBytesPerFile * float64(total-present)
	seconds := int((bytesLeft / 1024.0 / 1024.0) / speed)
	if seconds < 1 {
		seconds = 1
	}
	return fmt.Sprintf("%ds", seconds)
}

func dirRaceProgress(bridge MasterBridge, cfg *Config, dirPath string) (totalBytes int64, present int, total int) {
	if bridge == nil || cfg == nil {
		return 0, 0, 0
	}
	if useZipRaceMode(bridge, cfg, dirPath, "") {
		entries := bridge.ListDir(dirPath)
		expected := zipExpectedPartsFromDIZ(bridge, dirPath)
		_, totalBytes, present = zipDirRaceStats(bridge, dirPath, entries, expected)
		if expected > 0 {
			total = expected
		}
		return totalBytes, present, total
	}
	_, _, totalBytes, present, total = bridge.GetVFSRaceStats(dirPath)
	return totalBytes, present, total
}

func dirRaceStatusName(bridge MasterBridge, cfg *Config, dirPath, siteName string) string {
	if !raceStatusEligibleDir(dirPath) {
		return ""
	}
	var statusEntries []string
	totalBytes, present, total := dirRaceProgress(bridge, cfg, dirPath)
	extra := listStatusAudioExtra(bridge, cfg, dirPath)
	if total > 0 {
		totalMB := float64(totalBytes) / (1024 * 1024)
		if present >= total {
			if extra != "" {
				statusEntries = append(statusEntries, fmt.Sprintf("[%s] - ( %.0fM %dF - COMPLETE - %s ) - [%s]", siteName, totalMB, total, extra, siteName))
				extra = ""
			} else {
				statusEntries = append(statusEntries, fmt.Sprintf("[%s] - ( %.0fM %dF - COMPLETE ) - [%s]", siteName, totalMB, total, siteName))
			}
		} else {
			pct := (present * 100) / total
			if extra != "" {
				statusEntries = append(statusEntries, fmt.Sprintf("[%s] - ( %s %3d%% COMPLETE - %s ) - [%s]", siteName, progressBar(present, total, 20), pct, extra, siteName))
				extra = ""
			} else {
				statusEntries = append(statusEntries, fmt.Sprintf("[%s] - ( %s %3d%% COMPLETE ) - [%s]", siteName, progressBar(present, total, 20), pct, siteName))
			}
		}
	}
	if extra != "" {
		statusEntries = append(statusEntries, extra)
	}
	switch len(statusEntries) {
	case 0:
		return ""
	case 1:
		if total > 0 {
			return statusEntries[0]
		}
		return "[" + statusEntries[0] + "]"
	default:
		return strings.Join(statusEntries, " | ")
	}
}

func listStatusAudioExtra(bridge MasterBridge, cfg *Config, dirPath string) string {
	if bridge == nil || cfg == nil {
		return ""
	}
	if !cfg.Zipscript.Enabled || !cfg.Zipscript.Audio.Enabled {
		return ""
	}
	section, _ := zipscript.SectionInfoFromPath(dirPath)
	switch strings.ToUpper(strings.TrimSpace(section)) {
	case "MP3", "FLAC":
	default:
		return ""
	}
	info := bridge.GetDirMediaInfo(dirPath)
	if !zipscript.AudioInfoLooksUsable(info) {
		return ""
	}
	genre := firstNonEmptyMap(info, "genre", "g_genre")
	year := normalizeAudioYearForStatus(firstNonEmptyMap(info, "year", "g_recordeddate", "g_recorded_date", "g_originalreleaseddate", "g_original_released_date"))
	switch {
	case genre != "" && year != "":
		return genre + " " + year
	case genre != "":
		return genre
	default:
		return year
	}
}

func normalizeAudioYearForStatus(value string) string {
	value = strings.TrimSpace(value)
	if len(value) >= 4 {
		year := value[:4]
		allDigits := true
		for _, r := range year {
			if r < '0' || r > '9' {
				allDigits = false
				break
			}
		}
		if allDigits {
			return year
		}
	}
	return value
}

func raceStatusEligibleDir(dirPath string) bool {
	cleaned := path.Clean("/" + strings.TrimSpace(dirPath))
	if cleaned == "/" || cleaned == "." {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(cleaned, "/"), "/")
	if len(parts) < 2 {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(parts[0])) {
	case "FOREIGN", "PRE", "ARCHIVE":
		return len(parts) >= 3
	}
	return true
}

func emitRaceEndAfter(s *Session, dirPath string, users []VFSRaceUser, groups []VFSRaceGroup, totalBytes int64, total int, raceDurationMs int64, xferMs int64, delay time.Duration) {
	if delay > 0 {
		time.Sleep(delay)
	}
	emitRaceEnd(s, dirPath, users, groups, totalBytes, total, raceDurationMs, xferMs)
}

type releasePostHookQueue struct {
	tasks chan func()
}

var (
	releasePostHookQueuesMu sync.Mutex
	releasePostHookQueues   = map[string]*releasePostHookQueue{}
)

func enqueueReleasePostHook(dirPath string, task func()) {
	if task == nil {
		return
	}
	key := path.Clean("/" + dirPath)
	if key == "." || key == "" {
		key = "/"
	}

	releasePostHookQueuesMu.Lock()
	q := releasePostHookQueues[key]
	if q == nil {
		q = &releasePostHookQueue{tasks: make(chan func(), 128)}
		releasePostHookQueues[key] = q
		go runReleasePostHookQueue(key, q)
	}
	releasePostHookQueuesMu.Unlock()

	q.tasks <- task
}

func runReleasePostHookQueue(key string, q *releasePostHookQueue) {
	idle := time.NewTimer(30 * time.Second)
	defer idle.Stop()

	for {
		select {
		case task := <-q.tasks:
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			if task != nil {
				task()
			}
			idle.Reset(30 * time.Second)
		case <-idle.C:
			releasePostHookQueuesMu.Lock()
			if releasePostHookQueues[key] == q && len(q.tasks) == 0 {
				delete(releasePostHookQueues, key)
				releasePostHookQueuesMu.Unlock()
				return
			}
			releasePostHookQueuesMu.Unlock()
			idle.Reset(30 * time.Second)
		}
	}
}

type releaseUploadPipelineInput struct {
	UploadDir        string
	MediaInfoDir     string
	FilePath         string
	FileName         string
	Checksum         uint32
	TransferredBytes int64
	FileSize         int64
	SpeedMB          float64
	XferMs           int64
	CompletedAtMs    int64
	ExistingNames    []string
}

type releaseUploadPipelineState struct {
	SFVUpload        bool
	SFVEntries       map[string]uint32
	HadAudioInfo     bool
	HadMediaInfo     bool
	AudioFields      map[string]string
	MediaFields      map[string]string
	RaceUsers        []VFSRaceUser
	RaceGroups       []VFSRaceGroup
	RaceTotalBytes   int64
	RaceTotalFiles   int
	RaceDurationMs   int64
	RaceComplete     bool
	EventData        map[string]string
	ShouldAnnounceNR bool
}

type releaseProgressCacheBridge interface {
	CacheReleaseProgress(dirPath string, present, total int, hasManifest bool)
}

func runReleaseUploadPipeline(s *Session, bridge MasterBridge, in releaseUploadPipelineInput) {
	if s == nil || s.Config == nil || bridge == nil {
		return
	}
	if !finalizeReleaseUpload(s, bridge, in) {
		return
	}

	state := buildReleaseUploadPipelineState(s, bridge, in)
	emitReleaseUploadMetadata(s, bridge, in, state)
	emitReleaseUploadEventAndRace(s, bridge, in, state)
}

func finalizeReleaseUpload(s *Session, bridge MasterBridge, in releaseUploadPipelineInput) bool {
	if s == nil || s.Config == nil || bridge == nil {
		return false
	}

	if badZip, err := checkUploadedZipIntegrity(bridge, s.Config, in.UploadDir, in.FilePath, in.FileName); err != nil && s.Config.Debug {
		log.Printf("[MASTER-ZS] zip integrity check skipped for %s: %v", in.FilePath, err)
	} else if badZip {
		return false
	}

	if in.Checksum > 0 && zipscript.ShouldDeleteBadCRCForDir(s.Config.Zipscript, in.UploadDir) && !strings.HasSuffix(strings.ToLower(in.FileName), ".sfv") {
		sfvEntries := bridge.GetSFVData(in.UploadDir)
		if sfvEntries != nil {
			if expectedCRC, exists := cachedExpectedCRC(sfvEntries, in.FileName); exists {
				if expectedCRC != in.Checksum {
					bridge.DeleteFile(in.FilePath)
					_ = bridge.MarkFileMissing(in.FilePath)
					createMasterSFVMissingMarker(s.Config, bridge, in.UploadDir, in.FileName)
					log.Printf("[MASTER-ZS] CRC mismatch for %s: got %08X, expected %08X - deleted",
						in.FileName, in.Checksum, expectedCRC)
					return false
				}
				clearMasterSFVMissingMarker(bridge, in.UploadDir, in.FileName)
			}
		}
	}

	if s.User != nil && in.TransferredBytes > 0 {
		isSpeedtest := isSpeedtestPath(in.FilePath)
		s.User.UpdateStatsWithCredits(in.TransferredBytes, true, !isSpeedtest)
	}

	return true
}

func buildReleaseUploadPipelineState(s *Session, bridge MasterBridge, in releaseUploadPipelineInput) releaseUploadPipelineState {
	state := releaseUploadPipelineState{
		SFVUpload: strings.HasSuffix(strings.ToLower(in.FileName), ".sfv"),
		EventData: map[string]string{},
	}

	if state.SFVUpload {
		if sfvEntries, err := bridge.GetSFVInfo(in.FilePath); err == nil {
			log.Printf("[MASTER-ZS] Parsed SFV %s: %d entries", in.FileName, len(sfvEntries))
			bridge.CacheSFV(in.UploadDir, in.FileName, sfvEntries)
		}
	}
	state.SFVEntries = bridge.GetSFVData(in.UploadDir)
	if state.SFVEntries != nil {
		if state.SFVUpload {
			syncMasterSFVMissingMarkers(s.Config, bridge, in.UploadDir)
			bridge.SyncStatusMarkersForPath(in.UploadDir, true)
		} else {
			// Payload uploads only need their own marker cleared; full SFV rebuilds
			// are O(files in release) and run when the SFV is parsed or rescanned.
			clearMasterSFVMissingMarker(bridge, in.UploadDir, in.FileName)
		}
	}

	if err := refreshZipDIZFromArchive(bridge, in.UploadDir, in.FilePath, in.FileName); err != nil && s.Config.Debug {
		log.Printf("[MASTER-ZS] zip diz refresh skipped for %s: %v", in.FilePath, err)
	}

	state.HadAudioInfo = zipscript.AudioInfoLooksUsable(bridge.GetDirMediaInfo(in.UploadDir))
	state.HadMediaInfo = releaseMediaInfoLooksUsable(bridge.GetDirMediaInfo(in.MediaInfoDir))

	audioFields, err := applyAudioZipscriptChecksForDir(s, bridge, in.UploadDir, in.FilePath, in.FileName)
	if err != nil {
		log.Printf("[MASTER-ZS] post-upload audio check failed for %s: %v", in.FilePath, err)
	} else {
		state.AudioFields = cloneStringMap(audioFields)
	}
	state.MediaFields = probeSTORSitebotMediaInfo(s, bridge, in.MediaInfoDir, in.FilePath, in.FileName, state.HadMediaInfo)

	if subdir := zipscript.ReleaseSubdirLabel(s.Config.Zipscript, in.UploadDir); subdir != "" {
		state.EventData["release_subdir"] = subdir
		state.EventData["release_name"] = path.Base(path.Dir(in.UploadDir))
		if zipscript.IsIgnoredReleaseSubdir(s.Config.Zipscript, in.UploadDir) || !zipscript.AnnounceReleaseSubdirs(s.Config.Zipscript) {
			state.EventData["skip_release_announce"] = "true"
		}
	}
	if state.SFVUpload && state.SFVEntries != nil {
		state.EventData["t_filecount"] = fmt.Sprintf("%d", len(state.SFVEntries))
		state.EventData["t_file_label"] = zipscript.ExpectedFileLabel(s.Config.Zipscript, in.UploadDir)
	}

	state.RaceUsers, state.RaceGroups, state.RaceTotalBytes, state.RaceTotalFiles, state.RaceDurationMs, state.RaceComplete = computeReleaseRaceSnapshot(s, bridge, in, state.EventData)
	state.ShouldAnnounceNR = shouldAnnounceNoRace(s.Config, in.UploadDir, append([]string(nil), in.ExistingNames...), in.FileName)
	return state
}

func computeReleaseRaceSnapshot(s *Session, bridge MasterBridge, in releaseUploadPipelineInput, data map[string]string) ([]VFSRaceUser, []VFSRaceGroup, int64, int, int64, bool) {
	if s == nil || s.Config == nil || bridge == nil || !zipscript.RaceStatsOnSTORForDir(s.Config.Zipscript, in.UploadDir) {
		return nil, nil, 0, 0, 0, false
	}

	// pzs-ng/DrFTPD-style race wall-clock should start from the first real
	// uploaded file in the release, not only from tracked payload parts after
	// the SFV is known. Payload filtering still decides race completeness and
	// per-user file totals below.
	if strings.TrimSpace(in.FileName) != "" {
		bridge.NoteRacePayloadTransferAt(in.UploadDir, in.FileName, in.XferMs, in.CompletedAtMs)
	}

	trackedFile := in.FileName
	if strings.HasSuffix(strings.ToLower(in.FileName), ".sfv") {
		trackedFile = firstTrackedRaceFileName(bridge, in.UploadDir)
	}
	return populateUploadRaceData(bridge, s.Config, in.UploadDir, trackedFile, in.FileSize, data)
}

func emitReleaseUploadMetadata(s *Session, bridge MasterBridge, in releaseUploadPipelineInput, state releaseUploadPipelineState) {
	if state.AudioFields != nil {
		emitSTORSitebotAudioInfo(s, bridge, in.UploadDir, in.FilePath, in.FileName, in.TransferredBytes, in.SpeedMB, cloneStringMap(state.AudioFields), state.HadAudioInfo)
	}
	emitSTORSitebotMediaInfo(s, in.MediaInfoDir, in.FilePath, in.FileName, in.TransferredBytes, in.SpeedMB, state.MediaFields, state.HadMediaInfo)
}

func emitReleaseUploadEventAndRace(s *Session, bridge MasterBridge, in releaseUploadPipelineInput, state releaseUploadPipelineState) {
	userName := ""
	if s.User != nil {
		userName = s.User.Name
	}
	enrichUploadRaceUserData(state.EventData, state.RaceUsers, userName)
	s.emitEvent(EventUpload, in.FilePath, in.FileName, in.TransferredBytes, in.SpeedMB, state.EventData)

	if state.ShouldAnnounceNR && zipscript.RaceStatsOnSTORForDir(s.Config.Zipscript, in.UploadDir) {
		emitRaceEndAfter(s, in.UploadDir, nil, nil, in.FileSize, 1, 0, in.XferMs, 0)
		return
	}

	if useZipRaceMode(bridge, s.Config, in.UploadDir, in.FileName) {
		if state.RaceComplete && state.RaceTotalFiles > 0 {
			emitRaceEndAfter(s, in.UploadDir, state.RaceUsers, state.RaceGroups, state.RaceTotalBytes, state.RaceTotalFiles, state.RaceDurationMs, in.XferMs, zipscript.MediaInfoGraceDelayForDir(s.Config.Zipscript, in.UploadDir, in.FileName))
		}
		return
	}

	if state.SFVEntries == nil || !state.RaceComplete {
		return
	}
	if state.SFVUpload || zipscript.CanTriggerRaceEndForDir(s.Config.Zipscript, in.UploadDir, state.SFVEntries, in.FileName) {
		if err := bridge.SyncReleaseRaceStats(in.UploadDir); err != nil && s.Config.Debug {
			log.Printf("[MASTER-ZS] release race sync failed for %s: %v", in.UploadDir, err)
		}
		if state.AudioFields == nil {
			emitOrPrimeReleaseAudioInfo(s, bridge, in.UploadDir)
		}
		emitRaceEndAfter(s, in.UploadDir, state.RaceUsers, state.RaceGroups, state.RaceTotalBytes, state.RaceTotalFiles, state.RaceDurationMs, in.XferMs, zipscript.MediaInfoGraceDelayForDir(s.Config.Zipscript, in.UploadDir, in.FileName))
	}
}

func queueMasterUploadPostHooks(s *Session, bridge MasterBridge, uploadDir, mediaInfoDir, filePath, fileName string, checksum uint32, transferredBytes, fileSize int64, speedMB float64, xferMs int64, existingNames []string) {
	if s == nil || s.Config == nil || bridge == nil {
		return
	}
	input := releaseUploadPipelineInput{
		UploadDir:        uploadDir,
		MediaInfoDir:     mediaInfoDir,
		FilePath:         filePath,
		FileName:         fileName,
		Checksum:         checksum,
		TransferredBytes: transferredBytes,
		FileSize:         fileSize,
		SpeedMB:          speedMB,
		XferMs:           xferMs,
		CompletedAtMs:    time.Now().UnixMilli(),
		ExistingNames:    append([]string(nil), existingNames...),
	}
	enqueueReleasePostHook(uploadDir, func() {
		runReleaseUploadPipeline(s, bridge, input)
	})
}

func zipscriptExistingNames(bridge MasterBridge, dirPath string) []string {
	return zipscriptExistingNamesFromEntries(bridge.ListDir(dirPath))
}

func zipscriptExistingNamesFromEntries(entries []MasterFileEntry) []string {
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.Name)
	}
	return out
}

func zipscriptExistingDirNames(bridge MasterBridge, dirPath string) []string {
	return zipscriptExistingDirNamesFromEntries(bridge.ListDir(dirPath))
}

func zipscriptExistingDirNamesFromEntries(entries []MasterFileEntry) []string {
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir {
			out = append(out, entry.Name)
		}
	}
	return out
}

func existingFileNamesForXDupe(entries []MasterFileEntry) []string {
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir {
			continue
		}
		out = append(out, entry.Name)
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.ToLower(out[i]) < strings.ToLower(out[j])
	})
	return out
}

func xdupeResponseLines(mode int, names []string) []string {
	if mode <= 0 || len(names) == 0 {
		return nil
	}
	const prefix = "X-DUPE: "
	switch mode {
	case 1:
		lines := make([]string, 0, len(names))
		current := ""
		for _, name := range names {
			if len(name) > 66 {
				lines = append(lines, prefix+name[:65])
				continue
			}
			if len(current)+len(name) > 66 {
				if current != "" {
					lines = append(lines, prefix+current)
				}
				current = name
				continue
			}
			if current != "" {
				current += " "
			}
			current += name
		}
		if current != "" {
			lines = append(lines, prefix+current)
		}
		return lines
	case 2:
		lines := make([]string, 0, len(names))
		for _, name := range names {
			if len(name) > 66 {
				lines = append(lines, prefix+name[:65])
			} else {
				lines = append(lines, prefix+name)
			}
		}
		return lines
	case 3:
		lines := make([]string, 0, len(names))
		for _, name := range names {
			lines = append(lines, prefix+name)
		}
		return lines
	case 4:
		current := ""
		for _, name := range names {
			next := name
			if current != "" {
				next = current + " " + name
			}
			if len(next) > 1010 {
				break
			}
			current = next
		}
		if current == "" {
			return nil
		}
		return []string{prefix + current}
	default:
		return nil
	}
}

func isDuplicateUploadErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.HasPrefix(msg, "file ") && strings.HasSuffix(msg, " exists")
}

func writeDuplicateUploadResponse(s *Session, bridge MasterBridge, uploadDir, fileName string, err error) bool {
	if s == nil || s.Conn == nil || bridge == nil || !isDuplicateUploadErr(err) {
		return false
	}
	if s.Config != nil && s.Config.XdupeEnabled {
		for _, line := range xdupeResponseLines(s.XDupeMode, existingFileNamesForXDupe(bridge.ListDir(uploadDir))) {
			fmt.Fprintf(s.Conn, "553-%s\r\n", line)
		}
		fmt.Fprintf(s.Conn, "553 %s: file already exists (X-DUPE)\r\n", fileName)
		return true
	}
	fmt.Fprintf(s.Conn, "553 %s: file already exists\r\n", fileName)
	return true
}

func activeIncompleteIndicator(cfg *Config) string {
	if cfg == nil {
		return ""
	}
	return zipscript.IncompleteIndicator(cfg.Zipscript, "")
}

func trimRaceUsers(cfg *Config, users []VFSRaceUser) []VFSRaceUser {
	if cfg != nil && cfg.Zipscript.Race.MaskUserGroupNames {
		masked := make([]VFSRaceUser, len(users))
		copy(masked, users)
		for i := range masked {
			masked[i].Name = maskRaceName(masked[i].Name)
			masked[i].Group = maskRaceName(masked[i].Group)
		}
		users = masked
	}
	if cfg == nil || cfg.Zipscript.Race.MaxUsersInTop <= 0 || len(users) <= cfg.Zipscript.Race.MaxUsersInTop {
		return users
	}
	return users[:cfg.Zipscript.Race.MaxUsersInTop]
}

func trimRaceGroups(cfg *Config, groups []VFSRaceGroup) []VFSRaceGroup {
	if cfg != nil && cfg.Zipscript.Race.MaskUserGroupNames {
		masked := make([]VFSRaceGroup, len(groups))
		copy(masked, groups)
		for i := range masked {
			masked[i].Name = maskRaceName(masked[i].Name)
		}
		groups = masked
	}
	if cfg == nil || cfg.Zipscript.Race.MaxGroupsInTop <= 0 || len(groups) <= cfg.Zipscript.Race.MaxGroupsInTop {
		return groups
	}
	return groups[:cfg.Zipscript.Race.MaxGroupsInTop]
}

func maskRaceName(name string) string {
	runes := []rune(strings.TrimSpace(name))
	switch len(runes) {
	case 0:
		return name
	case 1:
		return string(runes)
	case 2:
		return string(runes[0]) + "*"
	default:
		return string(runes[0]) + strings.Repeat("*", len(runes)-2) + string(runes[len(runes)-1])
	}
}

func raceCRCKey(name string) string {
	name = strings.TrimSpace(path.Base(strings.ReplaceAll(name, "\\", "/")))
	return strings.ToLower(name)
}

func activeUploadForPath(filePath string) bool {
	cleanPath := path.Clean(filePath)
	for _, snap := range listActiveSessions() {
		if snap.TransferDirection != "upload" {
			continue
		}
		if path.Clean(snap.TransferPath) == cleanPath {
			return true
		}
	}
	return false
}

func activeUploadForPathWithBridge(bridge MasterBridge, filePath string) bool {
	if activeUploadForPath(filePath) {
		return true
	}
	if bridge == nil {
		return false
	}
	cleanPath := path.Clean(filePath)
	for _, stat := range bridge.GetLiveTransferStats() {
		if stat.Direction != "upload" {
			continue
		}
		if path.Clean(stat.Path) == cleanPath {
			return true
		}
	}
	return false
}

func cachedExpectedCRC(sfvEntries map[string]uint32, fileName string) (uint32, bool) {
	if sfvEntries == nil {
		return 0, false
	}
	crc, ok := sfvEntries[raceCRCKey(fileName)]
	return crc, ok
}

func shouldTreatDownloadAsMissing(cfg *Config, bridge MasterBridge, filePath string) bool {
	if cfg == nil || bridge == nil {
		return false
	}
	dirPath := path.Dir(filePath)
	expectedCRC, exists := cachedExpectedCRC(bridge.GetSFVData(dirPath), path.Base(filePath))
	if !exists || expectedCRC == 0 {
		return false
	}

	if knownCRC, ok := bridge.GetKnownChecksum(filePath); ok {
		if knownCRC == expectedCRC {
			return false
		}
		if zipscript.ShouldDeleteBadCRCForDir(cfg.Zipscript, dirPath) {
			if err := bridge.DeleteFile(filePath); err != nil && cfg.Debug {
				log.Printf("[MASTER-ZS] cached bad CRC delete failed for %s: %v", filePath, err)
			}
			_ = bridge.MarkFileMissing(filePath)
			missingPath := filePath + "-MISSING"
			if bridge.GetFileSize(missingPath) < 0 {
				_ = bridge.WriteFile(missingPath, []byte{})
			}
		}
		return true
	}

	// Do not run an extra checksum pass on RETR.
	// If no cached checksum is known yet, trust the upload-time verification path.
	return false
}

func syncMasterSFVMissingMarkers(cfg *Config, bridge MasterBridge, dirPath string) {
	if cfg == nil || bridge == nil || !zipscript.ShowMissingFilesForDir(cfg.Zipscript, dirPath) {
		return
	}
	sfvEntries := bridge.GetSFVData(dirPath)
	if sfvEntries == nil {
		return
	}
	verifiedPresent := bridge.GetVerifiedSFVPresentFiles(dirPath)
	existingFiles := map[string]bool{}
	for _, entry := range bridge.ListDir(dirPath) {
		if entry.IsDir || strings.HasSuffix(strings.ToUpper(strings.TrimSpace(entry.Name)), "-MISSING") {
			continue
		}
		existingFiles[raceCRCKey(entry.Name)] = true
	}
	for fileName := range sfvEntries {
		key := raceCRCKey(fileName)
		missingPath := path.Join(dirPath, fileName+"-MISSING")
		if (verifiedPresent != nil && verifiedPresent[key]) || existingFiles[key] {
			if bridge.GetFileSize(missingPath) >= 0 {
				_ = bridge.DeleteFile(missingPath)
			}
			continue
		}
		if bridge.GetFileSize(missingPath) < 0 {
			_ = bridge.WriteFile(missingPath, []byte{})
		}
	}
}

func clearMasterSFVMissingMarker(bridge MasterBridge, dirPath, fileName string) {
	if bridge == nil {
		return
	}
	missingPath := path.Join(dirPath, fileName+"-MISSING")
	if bridge.GetFileSize(missingPath) >= 0 {
		_ = bridge.DeleteFile(missingPath)
	}
}

func createMasterSFVMissingMarker(cfg *Config, bridge MasterBridge, dirPath, fileName string) {
	if cfg == nil || bridge == nil || !zipscript.ShowMissingFilesForDir(cfg.Zipscript, dirPath) {
		return
	}
	missingPath := path.Join(dirPath, fileName+"-MISSING")
	if bridge.GetFileSize(missingPath) < 0 {
		_ = bridge.WriteFile(missingPath, []byte{})
	}
}

func mediaInfoPluginSettings(cfg *Config) (sections []string, sampleOnly bool, videoExts map[string]bool) {
	sections = []string{"TV", "X264-HD-1080P", "X264-HD-720P", "X264-SD", "X265", "BLURAY"}
	sampleOnly = true
	videoExts = extensionSetLower([]string{"mkv", "mp4", "avi", "m2ts"})
	if cfg == nil {
		return
	}
	if len(cfg.Zipscript.Media.Sections) > 0 {
		sections = append([]string(nil), cfg.Zipscript.Media.Sections...)
	}
	if cfg.Zipscript.Media.SampleOnly != nil {
		sampleOnly = *cfg.Zipscript.Media.SampleOnly
	}
	if len(cfg.Zipscript.Media.VideoExtensions) > 0 {
		videoExts = extensionSetLower(cfg.Zipscript.Media.VideoExtensions)
	}
	if cfg.Zipscript.Media.Enabled != nil && !*cfg.Zipscript.Media.Enabled {
		sections = nil
		videoExts = map[string]bool{}
		return
	}
	return
}

func interfaceStringSlice(raw interface{}) ([]string, bool) {
	switch v := raw.(type) {
	case []string:
		out := make([]string, 0, len(v))
		for _, s := range v {
			s = strings.TrimSpace(s)
			if s != "" {
				out = append(out, s)
			}
		}
		return out, len(out) > 0
	case []interface{}:
		out := make([]string, 0, len(v))
		for _, item := range v {
			s, ok := item.(string)
			if !ok {
				continue
			}
			s = strings.TrimSpace(s)
			if s != "" {
				out = append(out, s)
			}
		}
		return out, len(out) > 0
	default:
		return nil, false
	}
}

func extensionSetLower(values []string) map[string]bool {
	out := make(map[string]bool, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(value, ".")))
		if value != "" {
			out[value] = true
		}
	}
	return out
}

func mediaInfoSectionMatch(section string, patterns []string) bool {
	section = strings.ToLower(strings.TrimSpace(section))
	for _, pat := range patterns {
		pat = strings.ToLower(strings.TrimSpace(pat))
		if pat != "" && strings.Contains(section, pat) {
			return true
		}
	}
	return false
}

func isSampleMediaPath(filePath string) bool {
	lower := strings.ToLower(filePath)
	return strings.Contains(lower, "/sample/") || strings.Contains(lower, "/samples/") || strings.Contains(lower, ".sample.")
}

func normalizeReleaseMediaInfoFields(fields map[string]string) {
	if fields == nil {
		return
	}
	fields["year"] = normalizeReleaseMediaYear(fields["year"])
	fields["bitrate"] = normalizeReleaseMediaBitrate(fields["bitrate"])
	fields["sample_rate"] = normalizeReleaseMediaSampleRate(fields["sample_rate"])
	fields["channels"] = normalizeReleaseMediaChannels(fields["channels"])
	fields["duration"] = normalizeReleaseMediaDuration(fields["duration"])
}

func normalizeReleaseMediaYear(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 4 {
		year := s[:4]
		if _, err := strconv.Atoi(year); err == nil {
			return year
		}
	}
	return s
}

func normalizeReleaseMediaBitrate(s string) string {
	raw := strings.TrimSpace(s)
	if raw == "" {
		return raw
	}
	lower := strings.ToLower(raw)
	if strings.Contains(lower, "kb") || strings.Contains(lower, "mb") {
		return raw
	}
	digits := strings.NewReplacer(" ", "", ",", "", ".", "").Replace(raw)
	if n, err := strconv.Atoi(digits); err == nil && n > 0 {
		if n >= 1000 {
			return fmt.Sprintf("%dkbps", n/1000)
		}
		return fmt.Sprintf("%dbps", n)
	}
	return raw
}

func normalizeReleaseMediaSampleRate(s string) string {
	raw := strings.TrimSpace(s)
	lower := strings.ToLower(raw)
	if strings.Contains(lower, "hz") {
		return strings.TrimSuffix(strings.TrimSuffix(lower, " hz"), "hz")
	}
	return raw
}

func normalizeReleaseMediaChannels(s string) string {
	switch strings.TrimSpace(s) {
	case "1":
		return "Mono"
	case "2":
		return "Stereo"
	case "6":
		return "5.1"
	case "8":
		return "7.1"
	default:
		return strings.TrimSpace(s)
	}
}

func normalizeReleaseMediaDuration(s string) string {
	raw := strings.TrimSpace(s)
	if raw == "" {
		return raw
	}
	lower := strings.ToLower(raw)
	if strings.Contains(lower, "min") || strings.Contains(raw, ":") {
		return raw
	}
	if seconds, err := strconv.ParseFloat(raw, 64); err == nil && seconds > 0 {
		min := int(seconds) / 60
		sec := int(seconds) % 60
		if min > 0 {
			return fmt.Sprintf("%dm%02ds", min, sec)
		}
		return fmt.Sprintf("%ds", sec)
	}
	return raw
}

func releaseMediaInfoLooksUsable(fields map[string]string) bool {
	if len(fields) == 0 {
		return false
	}
	return strings.TrimSpace(firstNonEmptyMap(fields, "video_format", "audio_format", "duration", "width", "height")) != ""
}

func emitReleaseMetadataEvent(s *Session, evtType EventType, dirPath, filePath, fileName string, size int64, speedMB float64, fields map[string]string) {
	if s == nil || len(fields) == 0 {
		return
	}
	if evtType == EventMediaInfo {
		dirPath = storReleaseMediaDir(dirPath, filePath)
	}
	data := cloneStringMap(fields)
	if data == nil {
		data = map[string]string{}
	}
	data["filepath"] = filePath
	data["filename"] = fileName
	data["path"] = dirPath
	data["relname"] = path.Base(dirPath)
	s.emitEvent(evtType, dirPath, path.Base(dirPath), size, speedMB, data)
}

func storReleaseMediaDir(uploadDir, filePath string) string {
	cleanFileDir := path.Dir(path.Clean(filePath))
	lowerFileBase := strings.ToLower(path.Base(cleanFileDir))
	if lowerFileBase == "sample" || lowerFileBase == "samples" {
		parent := path.Dir(cleanFileDir)
		if parent != "." && parent != "" {
			return parent
		}
	}
	cleanDir := path.Clean(uploadDir)
	if cleanDir == "." || cleanDir == "/" || cleanDir == "" {
		return cleanDir
	}
	lowerBase := strings.ToLower(path.Base(cleanDir))
	if lowerBase == "sample" || lowerBase == "samples" {
		parent := path.Dir(cleanDir)
		if parent != "." && parent != "" {
			return parent
		}
	}
	return cleanDir
}

func emitSTORSitebotAudioInfo(s *Session, bridge MasterBridge, dirPath, filePath, fileName string, size int64, speedMB float64, fields map[string]string, hadAudioInfo bool) {
	if hadAudioInfo || s == nil || bridge == nil || !zipscript.AudioInfoLooksUsable(fields) || !zipscript.ShowAudioInfoOnSTORForDir(s.Config.Zipscript, dirPath, fields) {
		return
	}
	if !bridge.ClaimReleaseMetadataAnnouncement(dirPath, "audioinfo") {
		return
	}
	emitReleaseMetadataEvent(s, EventAudioInfo, dirPath, filePath, fileName, size, speedMB, fields)
}

func emitOrPrimeReleaseAudioInfo(s *Session, bridge MasterBridge, dirPath string) {
	if s == nil || bridge == nil {
		return
	}
	fields := bridge.GetDirMediaInfo(dirPath)
	if zipscript.AudioInfoLooksUsable(fields) && zipscript.ShowAudioInfoOnSTORForDir(s.Config.Zipscript, dirPath, fields) {
		emitSTORSitebotAudioInfo(s, bridge, dirPath, dirPath, path.Base(dirPath), 0, 0, fields, false)
		return
	}

	entries := bridge.ListDir(dirPath)
	sort.Slice(entries, func(i, j int) bool {
		return strings.ToLower(entries[i].Name) < strings.ToLower(entries[j].Name)
	})
	for _, entry := range entries {
		if entry.IsDir || entry.IsSymlink {
			continue
		}
		ext := strings.ToLower(strings.TrimPrefix(path.Ext(entry.Name), "."))
		if ext != "mp3" && ext != "flac" && ext != "m4a" && ext != "wav" {
			continue
		}
		filePath := path.Join(dirPath, entry.Name)
		fields, err := applyAudioZipscriptChecksForDir(s, bridge, dirPath, filePath, entry.Name)
		if err != nil {
			if s.Config != nil && s.Config.Debug {
				log.Printf("[MASTER-ZS] release audio prime failed for %s: %v", filePath, err)
			}
			return
		}
		emitSTORSitebotAudioInfo(s, bridge, dirPath, filePath, entry.Name, entry.Size, 0, fields, false)
		return
	}
}

func probeSTORSitebotMediaInfo(s *Session, bridge MasterBridge, dirPath, filePath, fileName string, hadMediaInfo bool) map[string]string {
	if hadMediaInfo || s == nil || bridge == nil || s.Config == nil {
		if s != nil && s.Config != nil && s.Config.Debug && hadMediaInfo {
			log.Printf("[MASTER-ZS] stor media probe skipped for %s: release already has cached media info", filePath)
		}
		return nil
	}
	sections, sampleOnly, videoExts := mediaInfoPluginSettings(s.Config)
	section := sectionFromPathWithConfig(s.Config, dirPath)
	if len(sections) > 0 && !mediaInfoSectionMatch(section, sections) {
		if s.Config.Debug {
			log.Printf("[MASTER-ZS] stor media probe skipped for %s: section %q not enabled", filePath, section)
		}
		return nil
	}
	ext := strings.ToLower(strings.TrimPrefix(path.Ext(fileName), "."))
	if !videoExts[ext] {
		if s.Config.Debug {
			log.Printf("[MASTER-ZS] stor media probe skipped for %s: extension %q not enabled", filePath, ext)
		}
		return nil
	}
	if sampleOnly && !isSampleMediaPath(filePath) {
		if s.Config.Debug {
			log.Printf("[MASTER-ZS] stor media probe skipped for %s: sample_only enabled and path is not a sample path", filePath)
		}
		return nil
	}
	fields, err := bridge.ProbeMediaInfo(filePath, "", 0)
	if err != nil {
		if s.Config.Debug {
			log.Printf("[MASTER-ZS] stor media probe skipped for %s: %v", filePath, err)
		}
		return nil
	}
	normalizeReleaseMediaInfoFields(fields)
	if !releaseMediaInfoLooksUsable(fields) {
		if s.Config.Debug {
			log.Printf("[MASTER-ZS] stor media probe skipped for %s: parser returned unusable metadata", filePath)
		}
		return nil
	}
	if s.Config.Debug {
		log.Printf("[MASTER-ZS] stor media probe emitted for %s: video=%q audio=%q width=%q height=%q duration=%q", filePath, strings.TrimSpace(fields["video_format"]), strings.TrimSpace(fields["audio_format"]), strings.TrimSpace(fields["width"]), strings.TrimSpace(fields["height"]), strings.TrimSpace(fields["duration"]))
	}
	bridge.CacheMediaInfo(dirPath, fields)
	return fields
}

func emitSTORSitebotMediaInfo(s *Session, dirPath, filePath, fileName string, size int64, speedMB float64, fields map[string]string, hadMediaInfo bool) {
	if hadMediaInfo || !releaseMediaInfoLooksUsable(fields) {
		return
	}
	emitReleaseMetadataEvent(s, EventMediaInfo, dirPath, filePath, fileName, size, speedMB, fields)
}

func applyAudioZipscriptChecks(s *Session, bridge MasterBridge, filePath, fileName string) (map[string]string, error) {
	return applyAudioZipscriptChecksForDir(s, bridge, s.CurrentDir, filePath, fileName)
}

func applyAudioZipscriptChecksForDir(s *Session, bridge MasterBridge, dirPath, filePath, fileName string) (map[string]string, error) {
	if !zipscript.AudioCheckEnabled(s.Config.Zipscript, dirPath, fileName) {
		return nil, nil
	}
	if cached := bridge.GetDirMediaInfo(dirPath); zipscript.AudioInfoLooksUsable(cached) {
		return cached, nil
	}
	fields, err := bridge.ProbeMediaInfo(filePath, "", 0)
	if err != nil {
		if s.Config.Debug {
			log.Printf("[MASTER-ZS] audio probe skipped for %s: %v", filePath, err)
		}
		return nil, nil
	}
	if !zipscript.AudioInfoLooksUsable(fields) {
		if s.Config.Debug {
			log.Printf("[MASTER-ZS] audio probe for %s was not usable for release metadata", filePath)
		}
		return fields, nil
	}
	bridge.CacheMediaInfo(dirPath, fields)
	if reasons := zipscript.ValidateAudioRelease(s.Config.Zipscript, fields); len(reasons) > 0 {
		_ = bridge.DeleteFile(filePath)
		return nil, fmt.Errorf(strings.Join(reasons, "; "))
	}
	if err := ensureAudioSortLinks(bridge, zipscript.AudioSortLinks(s.Config.Zipscript, dirPath, fields)); err != nil && s.Config.Debug {
		log.Printf("[MASTER-ZS] audio sort link failed for %s: %v", dirPath, err)
	}
	return fields, nil
}

func emitPrefixedCommentLines(conn net.Conn, prefix string, lines []string) {
	if conn == nil {
		return
	}
	for _, line := range lines {
		line = strings.TrimRight(strings.ReplaceAll(line, "\r\n", "\n"), "\n")
		if line == "" {
			continue
		}
		for _, part := range strings.Split(line, "\n") {
			fmt.Fprintf(conn, "%s-%s\r\n", prefix, part)
		}
	}
}

func emitCWDZipDIZInfo(s *Session, bridge MasterBridge, dirPath string) {
	if s == nil || bridge == nil || !zipscript.ShowZipDIZOnCWDForDir(s.Config.Zipscript, dirPath) {
		return
	}
	content, err := bridge.ReadFile(path.Join(dirPath, "file_id.diz"))
	if err != nil || len(content) == 0 {
		recovered, recoverErr := recoverZipDIZFromDirectory(bridge, dirPath)
		if recoverErr != nil || len(recovered) == 0 {
			return
		}
		content = recovered
	}
	text := strings.ReplaceAll(string(content), "\r\n", "\n")
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	emitPrefixedCommentLines(s.Conn, "250", lines)
}

func emitCWDAudioInfo(s *Session, bridge MasterBridge, dirPath string) {
	if s == nil || bridge == nil {
		return
	}
	fields := bridge.GetDirMediaInfo(dirPath)
	if !zipscript.AudioInfoLooksUsable(fields) {
		if refreshed, ok := maybeBootstrapCWDAudioInfo(s, bridge, dirPath); ok {
			fields = refreshed
		}
	}
	if !zipscript.ShowAudioInfoOnCWDForDir(s.Config.Zipscript, dirPath, fields) {
		return
	}
	emitPrefixedCommentLines(s.Conn, "250", buildAudioInfoLines(dirPath, fields, false))
}

func maybeBootstrapCWDAudioInfo(s *Session, bridge MasterBridge, dirPath string) (map[string]string, bool) {
	if s == nil || bridge == nil || s.Config == nil {
		return nil, false
	}
	section, _ := zipscript.SectionInfoFromPath(dirPath)
	section = strings.ToUpper(strings.TrimSpace(section))
	audioEnabled := false
	switch section {
	case "MP3":
		audioEnabled = s.Config.Zipscript.Enabled && s.Config.Zipscript.Audio.Enabled &&
			s.Config.Zipscript.Audio.CWDMP3Info != nil && *s.Config.Zipscript.Audio.CWDMP3Info
	case "FLAC":
		audioEnabled = s.Config.Zipscript.Enabled && s.Config.Zipscript.Audio.Enabled &&
			s.Config.Zipscript.Audio.CWDFLACInfo != nil && *s.Config.Zipscript.Audio.CWDFLACInfo
	}
	if !audioEnabled {
		return nil, false
	}
	candidate, fields, ok := findFirstUsableAudioInfo(bridge, s.Config, dirPath)
	if !ok {
		return nil, false
	}
	previousFields := cloneStringMap(bridge.GetDirMediaInfo(dirPath))
	bridge.CacheMediaInfo(dirPath, fields)
	if err := refreshAudioSortLinks(bridge, s.Config.Zipscript, dirPath, previousFields, fields); err != nil && s.Config.Debug {
		log.Printf("[MASTER-ZS] cwd audio bootstrap sort link failed for %s: %v", dirPath, err)
	}
	if s.Config.Debug {
		log.Printf("[MASTER-ZS] cwd audio bootstrap refreshed %s from %s", dirPath, candidate)
	}
	return fields, true
}

func emitSTORAudioInfo(s *Session, dirPath string, fields map[string]string) {
	if s == nil || !zipscript.ShowAudioInfoOnSTORForDir(s.Config.Zipscript, dirPath, fields) {
		return
	}
	emitPrefixedCommentLines(s.Conn, "226", buildAudioInfoLines(dirPath, fields, true))
}

func buildAudioInfoLines(dirPath string, fields map[string]string, isStor bool) []string {
	if len(fields) == 0 {
		return nil
	}
	section := strings.ToUpper(strings.Trim(path.Clean(dirPath), "/"))
	if idx := strings.Index(section, "/"); idx >= 0 {
		section = section[:idx]
	}
	switch section {
	case "MP3":
		lines := []string{
			fmt.Sprintf("MP3 INFO: Artist: %s :: Album: %s :: Genre: %s :: Year: %s",
				audioDisplayField(fields, "artist", "g_performer", "g_album_performer"),
				audioDisplayField(fields, "album", "g_album"),
				audioDisplayField(fields, "genre", "g_genre"),
				audioDisplayField(fields, "year", "g_recordeddate", "g_recorded_date")),
		}
		if isStor {
			lines = append(lines,
				fmt.Sprintf("MP3 INFO: Track: %s :: Title: %s :: Bitrate: %s :: Freq: %s :: Mode: %s :: Runtime: %s",
					audioDisplayField(fields, "track", "g_track_name_position"),
					audioDisplayField(fields, "title", "g_track_name"),
					audioDisplayField(fields, "bitrate"),
					audioDisplayField(fields, "samplerate", "sampling_rate"),
					audioDisplayField(fields, "stereomode", "channel_s"),
					audioDisplayField(fields, "runtime", "duration")),
			)
		}
		return trimEmptyAudioLines(lines)
	case "FLAC":
		lines := []string{
			fmt.Sprintf("FLAC INFO: Artist: %s :: Album: %s :: Genre: %s :: Year: %s",
				audioDisplayField(fields, "artist", "g_performer", "g_album_performer"),
				audioDisplayField(fields, "album", "g_album"),
				audioDisplayField(fields, "genre", "g_genre"),
				audioDisplayField(fields, "year", "g_recordeddate", "g_recorded_date")),
		}
		if isStor {
			lines = append(lines,
				fmt.Sprintf("FLAC INFO: Track: %s :: Title: %s :: Freq: %s :: Channels: %s :: Runtime: %s",
					audioDisplayField(fields, "track", "g_track_name_position"),
					audioDisplayField(fields, "title", "g_track_name"),
					audioDisplayField(fields, "samplerate", "sampling_rate"),
					audioDisplayField(fields, "channels", "channel_s"),
					audioDisplayField(fields, "runtime", "duration")),
			)
		}
		return trimEmptyAudioLines(lines)
	default:
		return nil
	}
}

func audioDisplayField(values map[string]string, keys ...string) string {
	value := strings.TrimSpace(firstNonEmptyMap(values, keys...))
	if value == "" {
		return "unknown"
	}
	return value
}

func trimEmptyAudioLines(lines []string) []string {
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		normalized := strings.ReplaceAll(line, " ::  :: ", " :: ")
		normalized = strings.TrimSpace(normalized)
		if normalized != "" && !strings.HasSuffix(normalized, "INFO:") {
			out = append(out, normalized)
		}
	}
	return out
}

func firstNonEmptyMap(values map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(values[key]); value != "" {
			return value
		}
	}
	return ""
}

func ensureAudioSortLinks(bridge MasterBridge, links []zipscript.AudioSortLink) error {
	for _, link := range links {
		if err := ensureDirPath(bridge, link.DirPath); err != nil {
			return err
		}
		if err := bridge.Symlink(link.LinkPath, link.Target); err != nil {
			return err
		}
	}
	return nil
}

func ensureDirPathOwned(bridge MasterBridge, dirPath, owner, group string) error {
	dirPath = path.Clean("/" + strings.TrimSpace(dirPath))
	if dirPath == "/" || dirPath == "." {
		return nil
	}
	parts := strings.Split(strings.TrimPrefix(dirPath, "/"), "/")
	current := ""
	for _, part := range parts {
		if part == "" {
			continue
		}
		current = path.Join(current, "/"+part)
		if !bridge.FileExists(current) {
			if err := bridge.MakeDir(current, owner, group); err != nil {
				return err
			}
		}
	}
	return nil
}

func ensureDirPath(bridge MasterBridge, dirPath string) error {
	return ensureDirPathOwned(bridge, dirPath, "GoFTPd", "GoFTPd")
}

func ensureUploadDirsForEvent(s *Session, bridge MasterBridge, uploadDir string) error {
	if s == nil || s.Config == nil || bridge == nil {
		return nil
	}
	releaseDir := path.Clean("/" + strings.TrimSpace(uploadDir))
	if releaseDir == "/" || releaseDir == "." {
		return nil
	}
	if subdir := zipscript.ReleaseSubdirLabel(s.Config.Zipscript, releaseDir); subdir != "" {
		releaseDir = path.Dir(releaseDir)
	}
	needNew := !bridge.FileExists(releaseDir)
	owner := "GoFTPd"
	group := "GoFTPd"
	if s.User != nil {
		if strings.TrimSpace(s.User.Name) != "" {
			owner = s.User.Name
		}
		if strings.TrimSpace(s.User.PrimaryGroup) != "" {
			group = s.User.PrimaryGroup
		}
	}
	if err := ensureDirPathOwned(bridge, uploadDir, owner, group); err != nil {
		return err
	}
	if needNew {
		s.emitEvent(EventMKDir, releaseDir, path.Base(releaseDir), 0, 0, nil)
	}
	return nil
}

func shouldAnnounceNoRace(cfg *Config, dirPath string, existingNames []string, fileName string) bool {
	if cfg == nil || !cfg.Zipscript.Enabled || !cfg.Zipscript.Race.AnnounceNoRace {
		return false
	}
	if zipscript.IsIgnoredReleaseSubdir(cfg.Zipscript, dirPath) {
		return false
	}
	if zipscript.UsesRace(cfg.Zipscript, dirPath) || zipscript.IsIgnoredType(cfg.Zipscript, fileName) {
		return false
	}
	if strings.HasPrefix(strings.TrimSpace(fileName), ".") {
		return false
	}
	for _, name := range existingNames {
		name = strings.TrimSpace(name)
		if name == "" || strings.HasPrefix(name, ".") || zipscript.IsIgnoredType(cfg.Zipscript, name) {
			continue
		}
		return false
	}
	return true
}

func isZipPayloadName(name string) bool {
	return zipscript.IsZipPayloadName(name)
}

func isZipRecoverableArchiveName(name string) bool {
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(name)), ".zip")
}

func zipDirRaceStats(bridge MasterBridge, dirPath string, entries []MasterFileEntry, expectedTotal int) ([]VFSRaceUser, int64, int) {
	userMap := make(map[string]*VFSRaceUser)
	totalBytes := int64(0)
	total := 0
	for _, e := range entries {
		if e.IsDir || e.IsSymlink || strings.HasPrefix(strings.TrimSpace(e.Name), ".") || !isZipPayloadName(e.Name) {
			continue
		}
		if activeUploadForPathWithBridge(bridge, path.Join(dirPath, e.Name)) {
			continue
		}
		total++
		totalBytes += e.Size
		if e.XferTime <= 0 {
			continue
		}
		owner := e.Owner
		if owner == "" {
			owner = "unknown"
		}
		group := e.Group
		if group == "" {
			group = "NoGroup"
		}
		us := userMap[owner]
		if us == nil {
			us = &VFSRaceUser{Name: owner, Group: group}
			userMap[owner] = us
		}
		us.Files++
		us.Bytes += e.Size
		fileSpeed := float64(e.Size) / (float64(e.XferTime) / 1000.0)
		us.Speed += fileSpeed
		if fileSpeed > us.PeakSpeed {
			us.PeakSpeed = fileSpeed
		}
		if us.SlowSpeed == 0 || fileSpeed < us.SlowSpeed {
			us.SlowSpeed = fileSpeed
		}
		us.DurationMs += e.XferTime
	}
	users := make([]VFSRaceUser, 0, len(userMap))
	for _, us := range userMap {
		percentBase := total
		if expectedTotal > 0 {
			percentBase = expectedTotal
		}
		if percentBase > 0 {
			us.Percent = (us.Files * 100) / percentBase
		}
		if us.DurationMs > 0 {
			us.Speed = float64(us.Bytes) / (float64(us.DurationMs) / 1000.0)
		}
		users = append(users, *us)
	}
	sort.Slice(users, func(i, j int) bool {
		if users[i].Files != users[j].Files {
			return users[i].Files > users[j].Files
		}
		if users[i].Bytes != users[j].Bytes {
			return users[i].Bytes > users[j].Bytes
		}
		return strings.ToLower(users[i].Name) < strings.ToLower(users[j].Name)
	})
	return users, totalBytes, total
}

func addZipRaceEntry(users []VFSRaceUser, entry MasterFileEntry, expectedTotal int) ([]VFSRaceUser, int64) {
	if entry.IsDir || entry.IsSymlink || strings.HasPrefix(strings.TrimSpace(entry.Name), ".") || !isZipPayloadName(entry.Name) {
		return users, 0
	}

	owner := entry.Owner
	if owner == "" {
		owner = "unknown"
	}
	group := entry.Group
	if group == "" {
		group = "NoGroup"
	}

	found := false
	for i := range users {
		if users[i].Name != owner || users[i].Group != group {
			continue
		}
		users[i].Files++
		users[i].Bytes += entry.Size
		if entry.XferTime > 0 {
			fileSpeed := float64(entry.Size) / (float64(entry.XferTime) / 1000.0)
			users[i].DurationMs += entry.XferTime
			if fileSpeed > users[i].PeakSpeed {
				users[i].PeakSpeed = fileSpeed
			}
			if users[i].SlowSpeed == 0 || fileSpeed < users[i].SlowSpeed {
				users[i].SlowSpeed = fileSpeed
			}
			users[i].Speed = float64(users[i].Bytes) / (float64(users[i].DurationMs) / 1000.0)
		}
		found = true
		break
	}
	if !found {
		u := VFSRaceUser{
			Name:  owner,
			Group: group,
			Files: 1,
			Bytes: entry.Size,
		}
		if entry.XferTime > 0 {
			fileSpeed := float64(entry.Size) / (float64(entry.XferTime) / 1000.0)
			u.Speed = fileSpeed
			u.PeakSpeed = fileSpeed
			u.SlowSpeed = fileSpeed
			u.DurationMs = entry.XferTime
		}
		users = append(users, u)
	}

	percentBase := expectedTotal
	if percentBase <= 0 {
		percentBase = 1
	}
	for i := range users {
		users[i].Percent = (users[i].Files * 100) / percentBase
	}
	sort.Slice(users, func(i, j int) bool {
		if users[i].Files != users[j].Files {
			return users[i].Files > users[j].Files
		}
		if users[i].Bytes != users[j].Bytes {
			return users[i].Bytes > users[j].Bytes
		}
		return strings.ToLower(users[i].Name) < strings.ToLower(users[j].Name)
	})
	return users, entry.Size
}

func raceGroupsFromUsers(users []VFSRaceUser, totalFiles int) []VFSRaceGroup {
	groupMap := make(map[string]*VFSRaceGroup)
	for _, u := range users {
		group := strings.TrimSpace(u.Group)
		if group == "" {
			group = "NoGroup"
		}
		g := groupMap[group]
		if g == nil {
			g = &VFSRaceGroup{Name: group}
			groupMap[group] = g
		}
		g.Files += u.Files
		g.Bytes += u.Bytes
		g.Speed += u.Speed
	}
	groups := make([]VFSRaceGroup, 0, len(groupMap))
	for _, g := range groupMap {
		if totalFiles > 0 {
			g.Percent = (g.Files * 100) / totalFiles
		}
		groups = append(groups, *g)
	}
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].Bytes != groups[j].Bytes {
			return groups[i].Bytes > groups[j].Bytes
		}
		if groups[i].Files != groups[j].Files {
			return groups[i].Files > groups[j].Files
		}
		return strings.ToLower(groups[i].Name) < strings.ToLower(groups[j].Name)
	})
	return groups
}

func zipDirPayloadCount(bridge MasterBridge, dirPath string, entries []MasterFileEntry) int {
	total := 0
	for _, e := range entries {
		if e.IsDir || e.IsSymlink || strings.HasPrefix(strings.TrimSpace(e.Name), ".") || !isZipPayloadName(e.Name) {
			continue
		}
		if activeUploadForPathWithBridge(bridge, path.Join(dirPath, e.Name)) {
			continue
		}
		total++
	}
	return total
}

func zipExpectedPartsFromDIZ(bridge MasterBridge, dirPath string) int {
	content, err := bridge.ReadFile(path.Join(dirPath, "file_id.diz"))
	if err != nil || len(content) == 0 {
		recovered, recoverErr := recoverZipDIZFromDirectory(bridge, dirPath)
		if recoverErr != nil || len(recovered) == 0 {
			return 0
		}
		content = recovered
	}
	return zipExpectedPartsFromDIZContent(content)
}

func zipExpectedPartsFromDIZContent(content []byte) int {
	if len(content) == 0 {
		return 0
	}
	text := normalizeZipDIZText(string(content))
	if text == "" {
		return 0
	}

	patterns := []string{
		"[?!/##]",
		"(?!/##)",
		"[?!!/###]",
		"(?!!/###)",
		"[?/#]",
		"[?/##]",
		"(?/#)",
		"[disk:!!/##]",
		"[disk:?!/##]",
		"o?/o#",
		"disks[!!/##",
		" !/# ",
		" !!/##&/&!",
		"&/!!/## ",
		"[!!/#]",
		": ?!/##&/",
		"xx/##",
		"<!!/##>",
		"x/##",
		"! of #",
		"? of #",
		"x of #",
		"ox of o#",
		"!! of ##",
		"?! of ##",
		"xx of ##",
	}

	best := 0
	for start := 0; start < len(text); start++ {
		for _, pattern := range patterns {
			total, ok := matchZipDIZPattern(text, start, pattern)
			if ok && total > best {
				best = total
			}
		}
	}
	return best
}

func normalizeZipDIZText(text string) string {
	var b strings.Builder
	b.Grow(len(text))
	lastSpace := false
	for _, r := range text {
		switch r {
		case '\x00', '\r', '\n', '\t', ' ':
			if !lastSpace {
				b.WriteByte(' ')
				lastSpace = true
			}
		default:
			b.WriteRune(unicode.ToLower(r))
			lastSpace = false
		}
	}
	return b.String()
}

func matchZipDIZPattern(text string, start int, pattern string) (int, bool) {
	if start >= len(text) {
		return 0, false
	}
	var totalDigits strings.Builder
	control := 0
	matches := 0
	for patIdx := 0; patIdx <= len(pattern)-control-1; patIdx++ {
		textIdx := start + patIdx
		if textIdx >= len(text) {
			return 0, false
		}
		ch := text[textIdx]
		token := pattern[patIdx+control]
		switch token {
		case '#':
			if (ch >= '0' && ch <= '9') || ch == ' ' || ch == 'o' {
				if ch == 'o' {
					ch = '0'
				}
				matches++
				totalDigits.WriteByte(ch)
			}
		case '?':
			matches++
		case '!':
			if (ch >= '0' && ch <= '9') || ch == 'o' || ch == 'x' {
				matches++
			}
		case '&':
			control++
			if patIdx+control >= len(pattern) {
				return 0, false
			}
			next := pattern[patIdx+control]
			if !(next == '!' && ((ch >= '0' && ch <= '9') || ch == 'o' || ch == 'x')) && ch != next {
				matches++
			}
		default:
			if token == ch {
				matches++
			}
		}
	}
	if matches != len(pattern)-control {
		return 0, false
	}
	raw := strings.TrimSpace(totalDigits.String())
	if raw == "" {
		return 0, false
	}
	total, err := strconv.Atoi(raw)
	if err != nil || total <= 0 {
		return 0, false
	}
	return total, true
}

func recoverZipDIZFromDirectory(bridge MasterBridge, dirPath string) ([]byte, error) {
	if bridge == nil {
		return nil, fmt.Errorf("bridge unavailable")
	}
	entries := bridge.ListDir(dirPath)
	var archives []string
	for _, entry := range entries {
		if entry.IsDir {
			continue
		}
		if entry.Size <= 0 || entry.XferTime <= 0 {
			continue
		}
		if isZipRecoverableArchiveName(entry.Name) {
			archivePath := path.Join(dirPath, entry.Name)
			if activeUploadForPathWithBridge(bridge, archivePath) {
				continue
			}
			archives = append(archives, archivePath)
		}
	}
	sort.Strings(archives)
	for _, archivePath := range archives {
		content, err := bridge.ReadZipEntry(archivePath, "file_id.diz")
		if err != nil || len(content) == 0 {
			continue
		}
		if writeErr := bridge.WriteFile(path.Join(dirPath, "file_id.diz"), content); writeErr != nil {
			return nil, writeErr
		}
		return content, nil
	}
	return nil, fmt.Errorf("file_id.diz not found in any archive")
}

func shouldBlockZipDIZUpload(cfg *Config, dirPath, fileName string) bool {
	if cfg == nil || !zipscript.UsesZip(cfg.Zipscript, dirPath) {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(fileName), "file_id.diz")
}

func checkUploadedZipIntegrity(bridge MasterBridge, cfg *Config, dirPath, filePath, fileName string) (bool, error) {
	if bridge == nil || cfg == nil || !zipscript.CheckZipIntegrityForDir(cfg.Zipscript, dirPath) {
		return false, nil
	}
	if !strings.HasSuffix(strings.ToLower(strings.TrimSpace(fileName)), ".zip") {
		return false, nil
	}
	ok, err := bridge.CheckZipIntegrity(filePath)
	if err != nil {
		return false, err
	}
	if ok {
		return false, nil
	}
	bridge.DeleteFile(filePath)
	log.Printf("[MASTER-ZS] Zip integrity failed for %s — deleted", filePath)
	return true, nil
}

func refreshZipDIZFromArchive(bridge MasterBridge, dirPath, archivePath, fileName string) error {
	if bridge == nil || !isZipRecoverableArchiveName(fileName) {
		return nil
	}
	dizPath := path.Join(dirPath, "file_id.diz")
	if bridge.GetFileSize(dizPath) >= 0 {
		return nil
	}
	content, err := bridge.ReadZipEntry(archivePath, "file_id.diz")
	if err != nil || len(content) == 0 {
		return err
	}
	return bridge.WriteFile(dizPath, content)
}

func zipDirComplete(bridge MasterBridge, dirPath string, entries []MasterFileEntry, expected int) bool {
	if expected <= 0 {
		return false
	}
	total := zipDirPayloadCount(bridge, dirPath, entries)
	return total == expected
}

func zipDirCompleteAfterUpload(bridge MasterBridge, dirPath, fileName string, entries []MasterFileEntry, expected int) bool {
	if zipDirComplete(bridge, dirPath, entries, expected) {
		return true
	}
	if expected <= 0 || !isZipPayloadName(fileName) {
		return false
	}

	currentPath := path.Join(dirPath, fileName)
	if !activeUploadForPathWithBridge(bridge, currentPath) {
		return false
	}

	for _, e := range entries {
		if e.IsDir || e.IsSymlink {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(e.Name), strings.TrimSpace(fileName)) {
			continue
		}
		total := zipDirPayloadCount(bridge, dirPath, entries) + 1
		return total == expected
	}
	return false
}

func cacheZipReleaseProgress(bridge MasterBridge, dirPath string, present, total int) {
	if bridge == nil || total <= 0 {
		return
	}
	if cacher, ok := bridge.(releaseProgressCacheBridge); ok {
		cacher.CacheReleaseProgress(dirPath, present, total, true)
	}
	bridge.SyncStatusMarkersForPath(dirPath, true)
}

func populateUploadRaceData(bridge MasterBridge, cfg *Config, dirPath, fileName string, fileSize int64, data map[string]string) ([]VFSRaceUser, []VFSRaceGroup, int64, int, int64, bool) {
	type freshRaceStatsBridge interface {
		GetVFSRaceStatsFresh(dirPath string) (users []VFSRaceUser, groups []VFSRaceGroup, totalBytes int64, present int, total int)
	}

	sfvEntries := bridge.GetSFVData(dirPath)
	usesZip := useZipRaceMode(bridge, cfg, dirPath, fileName)
	isTrackedPayload := isTrackedRacePayload(bridge, cfg, dirPath, fileName)
	if !isTrackedPayload && !usesZip {
		return nil, nil, 0, 0, 0, false
	}
	if isTrackedPayload {
		data["file_mbytes"] = mbString(fileSize)
	}
	if usesZip {
		expected := zipExpectedPartsFromDIZ(bridge, dirPath)
		entries := bridge.ListDir(dirPath)
		users, totalBytes, total := zipDirRaceStats(bridge, dirPath, entries, expected)
		cacheZipReleaseProgress(bridge, dirPath, total, expected)
		raceComplete := zipDirCompleteAfterUpload(bridge, dirPath, fileName, entries, expected)
		if raceComplete && expected > 0 && total < expected {
			for _, entry := range entries {
				if !strings.EqualFold(strings.TrimSpace(entry.Name), strings.TrimSpace(fileName)) {
					continue
				}
				var addedBytes int64
				users, addedBytes = addZipRaceEntry(users, entry, expected)
				if addedBytes > 0 {
					totalBytes += addedBytes
					total++
				}
				break
			}
		}
		if total > 0 {
			raceDurationMs := bridge.GetRaceWallClockMilliseconds(dirPath)
			avgSpeedMB := aggregateRaceSpeedMB(users)
			if avgSpeedMB <= 0 {
				avgSpeedMB = currentRaceSpeedMB(dirPath, totalBytes, bridge)
			}
			if avgSpeedMB <= 0 {
				avgSpeedMB = raceSpeedMBForDuration(totalBytes, raceDurationMs)
			}
			totalFiles := total
			if expected > 0 {
				totalFiles = expected
			}
			groups := raceGroupsFromUsers(users, totalFiles)
			data["relname"] = path.Base(dirPath)
			if expected > 0 {
				data["t_files"] = fmt.Sprintf("%d", expected)
				data["t_present"] = fmt.Sprintf("%d", total)
				data["t_filesleft"] = fmt.Sprintf("%d", maxInt(0, expected-total))
			} else {
				delete(data, "t_files")
				delete(data, "t_present")
				delete(data, "t_filesleft")
			}
			data["t_totalmb"] = fmt.Sprintf("%.1f", float64(totalBytes)/1024.0/1024.0)
			data["t_avgspeed"] = fmt.Sprintf("%.2fMB/s", avgSpeedMB)
			if expected > 0 && expected > total {
				data["t_timeleft"] = estimateRaceTimeLeftWithSpeed(totalBytes, total, expected, avgSpeedMB)
			} else if expected > 0 {
				data["t_timeleft"] = "0s"
			} else {
				delete(data, "t_timeleft")
			}
			data["t_mbytes"] = fmt.Sprintf("%.0fMB", float64(totalBytes)/1024.0/1024.0)
			if len(users) > 0 {
				leader := users[0]
				data["leader_name"] = leader.Name
				data["leader_group"] = leader.Group
				data["leader_files"] = fmt.Sprintf("%d", leader.Files)
				data["leader_mb"] = fmt.Sprintf("%.1f", float64(leader.Bytes)/1024.0/1024.0)
				data["leader_pct"] = fmt.Sprintf("%d", leader.Percent)
				data["leader_speed"] = fmt.Sprintf("%.2fMB/s", leader.Speed/1024.0/1024.0)
			}
			return users, groups, totalBytes, totalFiles, raceDurationMs, raceComplete || expected == 0 || total >= expected
		}
		return nil, nil, 0, 0, 0, false
	}
	if sfvEntries != nil {
		var users []VFSRaceUser
		var groups []VFSRaceGroup
		var totalBytes int64
		var present, total int
		raceDurationMs := int64(0)
		if freshBridge, ok := bridge.(freshRaceStatsBridge); ok {
			users, groups, totalBytes, present, total = freshBridge.GetVFSRaceStatsFresh(dirPath)
		} else {
			users, groups, totalBytes, present, total = bridge.GetVFSRaceStats(dirPath)
		}
		raceDurationMs = bridge.GetRaceWallClockMilliseconds(dirPath)
		if total > 0 {
			avgSpeedMB := aggregateRaceSpeedMB(users)
			if avgSpeedMB <= 0 {
				avgSpeedMB = currentRaceSpeedMB(dirPath, totalBytes, bridge)
			}
			if avgSpeedMB <= 0 {
				avgSpeedMB = raceSpeedMBForDuration(totalBytes, raceDurationMs)
			}
			data["relname"] = path.Base(dirPath)
			data["t_files"] = fmt.Sprintf("%d", total)
			data["t_present"] = fmt.Sprintf("%d", present)
			data["t_filesleft"] = fmt.Sprintf("%d", maxInt(0, total-present))
			data["t_totalmb"] = fmt.Sprintf("%.1f", float64(totalBytes)/1024.0/1024.0)
			data["t_avgspeed"] = fmt.Sprintf("%.2fMB/s", avgSpeedMB)
			data["t_timeleft"] = estimateRaceTimeLeftWithSpeed(totalBytes, present, total, avgSpeedMB)
			estBytes := fileSize * int64(total)
			data["t_mbytes"] = fmt.Sprintf("%.0fMB", float64(estBytes)/1024.0/1024.0)
			if len(users) > 0 {
				leader := users[0]
				data["leader_name"] = leader.Name
				data["leader_group"] = leader.Group
				data["leader_files"] = fmt.Sprintf("%d", leader.Files)
				data["leader_mb"] = fmt.Sprintf("%.1f", float64(leader.Bytes)/1024.0/1024.0)
				data["leader_pct"] = fmt.Sprintf("%d", leader.Percent)
				data["leader_speed"] = fmt.Sprintf("%.2fMB/s", leader.Speed/1024.0/1024.0)
			}
			return users, groups, totalBytes, total, raceDurationMs, present >= total
		}
	}
	return nil, nil, 0, 0, 0, false
}

func isTrackedRacePayload(bridge MasterBridge, cfg *Config, dirPath, fileName string) bool {
	if bridge == nil || cfg == nil {
		return false
	}
	sfvEntries := bridge.GetSFVData(dirPath)
	isTrackedPayload := zipscript.IsRacePayloadFileForDir(cfg.Zipscript, dirPath, fileName)
	if sfvEntries != nil {
		_, isTrackedPayload = sfvEntries[strings.ToLower(strings.TrimSpace(path.Base(strings.ReplaceAll(fileName, "\\", "/"))))]
		if !isTrackedPayload {
			isTrackedPayload = zipscript.IsRacePayloadFileForDir(cfg.Zipscript, dirPath, fileName)
		}
	}
	return isTrackedPayload
}

func firstTrackedRaceFileName(bridge MasterBridge, dirPath string) string {
	sfvEntries := bridge.GetSFVData(dirPath)
	for name := range sfvEntries {
		if strings.TrimSpace(name) != "" {
			return name
		}
	}
	return ""
}

func enrichUploadRaceUserData(data map[string]string, users []VFSRaceUser, username string) {
	if data == nil || len(users) == 0 || strings.TrimSpace(username) == "" {
		return
	}
	for _, u := range users {
		if !strings.EqualFold(strings.TrimSpace(u.Name), strings.TrimSpace(username)) {
			continue
		}
		data["u_race_speed"] = fmt.Sprintf("%.2fMB/s", userDisplaySpeed(u)/1024.0/1024.0)
		data["u_race_files"] = fmt.Sprintf("%d", u.Files)
		data["u_race_mb"] = fmt.Sprintf("%.1f", float64(u.Bytes)/1024.0/1024.0)
		data["u_race_pct"] = fmt.Sprintf("%d", u.Percent)
		if strings.TrimSpace(data["u_group"]) == "" && strings.TrimSpace(u.Group) != "" {
			data["u_group"] = u.Group
		}
		return
	}
}

func shouldEmitZipRaceEnd(cfg *Config, dirPath, fileName string) bool {
	if cfg == nil || !zipscript.UsesZip(cfg.Zipscript, dirPath) {
		return false
	}
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(fileName)), ".zip")
}

func useZipRaceMode(bridge MasterBridge, cfg *Config, dirPath, fileName string) bool {
	if cfg == nil || !zipscript.UsesZip(cfg.Zipscript, dirPath) {
		return false
	}
	if !zipscript.UsesSFV(cfg.Zipscript, dirPath) {
		return true
	}
	if isZipPayloadName(fileName) || zipscript.IsZipManifestName(fileName) {
		return true
	}
	if bridge == nil {
		return false
	}
	if bridge.GetSFVData(dirPath) != nil {
		return false
	}
	for _, entry := range bridge.ListDir(dirPath) {
		if entry.IsDir || entry.IsSymlink {
			continue
		}
		if isZipPayloadName(entry.Name) || zipscript.IsZipManifestName(entry.Name) {
			return true
		}
	}
	return false
}

func (s *Session) resolvePretTargetPath(bridge MasterBridge) string {
	targetPath := s.CurrentDir
	if strings.TrimSpace(s.PretArg) != "" {
		if path.IsAbs(strings.TrimSpace(s.PretArg)) {
			targetPath = path.Clean(s.PretArg)
		} else {
			targetPath = path.Clean(path.Join(s.CurrentDir, s.PretArg))
		}
	}
	if bridge != nil {
		targetPath = bridge.ResolvePath(targetPath)
	}
	return targetPath
}
