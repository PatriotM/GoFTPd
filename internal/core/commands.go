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
					log.Printf("[CPSV] Passthrough slave listen failed: %v", err)
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
		s.PendingUser = args[0]
		s.PendingReason = ""
		s.User = nil
		u, err := user.LoadUser(args[0], s.GroupMap)
		if err != nil {
			if _, statErr := os.Stat(deletedUserPath(args[0])); statErr == nil {
				s.PendingReason = "user_deleted"
			} else {
				s.PendingReason = "unknown_user"
			}
			if s.Config.Debug {
				log.Printf("[AUTH] Failed to load user '%s': %v", args[0], err)
			}
		} else {
			s.User = u
		}
		fmt.Fprintf(s.Conn, "331 Password required for %s\r\n", args[0])

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
				s.emitLoginFailure(s.User.Name, remoteIP, "ip_not_allowed")
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

			s.IsLogged = true
			s.PendingUser = ""
			s.PendingReason = ""
			if strings.TrimSpace(s.User.CurrentDir) != "" {
				s.CurrentDir = path.Clean(s.User.CurrentDir)
			}
			s.User.LastLogin = time.Now().Unix()
			s.User.Save()
			fmt.Fprintf(s.Conn, "230-Welcome to GoFTPd, %s!\r\n", s.User.Name)
			fmt.Fprintf(s.Conn, "230-Tagline: %s\r\n", s.User.Tagline)

			s.showGlobalStats("230", false)
			fmt.Fprintf(s.Conn, "230 User logged in.\r\n")

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
		s.CurrentDir = path.Clean(path.Join(s.CurrentDir, ".."))
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
		aclPath := path.Join(s.Config.ACLBasePath, s.CurrentDir, args[0])
		if !s.ACLEngine.CanPerform(s.User, "RMD", aclPath) {
			fmt.Fprintf(s.Conn, "550 Access Denied: Insufficient flags.\r\n")
			return false
		}
		if s.Config.Mode == "master" && s.MasterManager != nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				dirPath := path.Join(s.CurrentDir, args[0])
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

				// Race-stats virtual entry — mirrors the [HV] - ( ... COMPLETE ) - [HV]
				// row that LIST shows. Rendered as Type=dir so it appears at the top
				// of client browsers the same way LIST's drwxr-xr-x row did.
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

				for _, marker := range incompleteMarkerEntries(bridge, s.Config, activeIncompleteIndicator(s.Config), targetPath, entries) {
					ts := timeutil.FTPMachineUnix(marker.ModTime)
					output.WriteString(fmt.Sprintf("Modify=%s;Perm=el;Type=%s; %s\r\n",
						ts, mlsdSymlinkType(marker), marker.Name))
				}

				for _, e := range entries {
					if strings.HasPrefix(e.Name, ".") {
						continue
					}
					if isIncompleteMarkerName(activeIncompleteIndicator(s.Config), e.Name) {
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
						if !isIncompleteMarkerName(activeIncompleteIndicator(s.Config), entry.Name) {
							output.WriteString(entry.Name + "\r\n")
						}
					}
				} else {
					for _, e := range bridge.ListDir(targetPath) {
						if strings.HasPrefix(e.Name, ".") {
							continue
						}
						if strings.HasSuffix(e.Name, "-missing") || strings.HasSuffix(e.Name, "-MISSING") {
							continue
						}
						if isIncompleteMarkerName(activeIncompleteIndicator(s.Config), e.Name) {
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
				if !strings.HasPrefix(name, ".") {
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

				for _, marker := range incompleteMarkerEntries(bridge, s.Config, activeIncompleteIndicator(s.Config), targetPath, entries) {
					ts := timeutil.Unix(marker.ModTime).Format("Jan _2 15:04")
					name := fmt.Sprintf("%s -> %s", marker.Name, marker.LinkTarget)
					output.WriteString(fmt.Sprintf("%s   1 %-8s %-8s %10s %s %s\r\n",
						ftpListMode(marker), marker.Owner, marker.Group, "0", ts, name))
				}

				for _, e := range entries {
					if strings.HasPrefix(e.Name, ".") {
						continue
					}
					if strings.HasSuffix(e.Name, "-missing") || strings.HasSuffix(e.Name, "-MISSING") {
						continue
					}
					if isIncompleteMarkerName(activeIncompleteIndicator(s.Config), e.Name) {
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
					output.WriteString(fmt.Sprintf("%s   1 %-8s %-8s %10s %s %s\r\n",
						mode, owner, group, size, ts, name))
				}

				if zipscript.ShowMissingFilesForDir(s.Config.Zipscript, targetPath) && total > 0 && present < total {
					sfvMeta := bridge.GetSFVData(targetPath)
					verifiedPresent := bridge.GetVerifiedSFVPresentFiles(targetPath)
					if sfvMeta != nil {
						for fileName := range sfvMeta {
							key := raceCRCKey(fileName)
							if verifiedPresent != nil {
								if verifiedPresent[key] {
									continue
								}
							} else if existingFiles[fileName] {
								continue
							}
							output.WriteString(fmt.Sprintf("-rw-r--r--   1 %-8s %-8s %10s %s %s-MISSING\r\n",
								"GoFTPd", "GoFTPd", "0", now, fileName))
						}
					}
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
					xdupeNames = existingFileNamesForXDupe(bridge.ListDir(uploadDir))
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
				existingNames = zipscriptExistingNames(bridge, uploadDir)
				existingDirs := zipscriptExistingDirNames(bridge, uploadDir)
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
					log.Printf("[Passthrough] PORT upload failed: %v", err)
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
				if badZip, err := checkUploadedZipIntegrity(bridge, s.Config, uploadDir, filePath, fileName); err != nil && s.Config.Debug {
					log.Printf("[MASTER-ZS] zip integrity check skipped for %s: %v", filePath, err)
				} else if badZip {
					fmt.Fprintf(s.Conn, "226 Zip integrity check failed, deleting file\r\n")
					return false
				}

				if checksum > 0 && zipscript.ShouldDeleteBadCRCForDir(s.Config.Zipscript, uploadDir) && !strings.HasSuffix(strings.ToLower(fileName), ".sfv") {
					sfvEntries := bridge.GetSFVData(uploadDir)
					if sfvEntries != nil {
						if expectedCRC, exists := cachedExpectedCRC(sfvEntries, fileName); exists {
							if expectedCRC != checksum {
								bridge.DeleteFile(filePath)
								log.Printf("[MASTER-ZS] CRC mismatch for %s: got %08X, expected %08X — deleted",
									fileName, checksum, expectedCRC)
								fmt.Fprintf(s.Conn, "226- checksum mismatch: SLAVE: %08X SFV: %08X\r\n", checksum, expectedCRC)
								fmt.Fprintf(s.Conn, "226 Checksum mismatch, deleting file\r\n")
								return false
							}
							if s.Config.Debug {
								log.Printf("[MASTER-ZS] CRC match for %s: %08X", fileName, checksum)
							}
						}
					}
				}
				if !strings.HasSuffix(strings.ToLower(fileName), ".sfv") {
					sfvEntries := bridge.GetSFVData(uploadDir)
					if expectedCRC, exists := cachedExpectedCRC(sfvEntries, fileName); exists {
						writeUploadSFVStatus(s.Conn, checksum, expectedCRC, true, fileSize)
					} else {
						writeUploadNoSFVEntryStatus(s.Conn, sfvEntries, fileName)
					}
				}

				if strings.HasSuffix(strings.ToLower(fileName), ".sfv") {
					if sfvEntries, err := bridge.GetSFVInfo(filePath); err == nil {
						log.Printf("[MASTER-ZS] Parsed SFV %s: %d entries", fileName, len(sfvEntries))
						bridge.CacheSFV(uploadDir, fileName, sfvEntries)
					}
				}
				if err := refreshZipDIZFromArchive(bridge, uploadDir, filePath, fileName); err != nil && s.Config.Debug {
					log.Printf("[MASTER-ZS] zip diz refresh skipped for %s: %v", filePath, err)
				}
				audioFields, err := applyAudioZipscriptChecksForDir(s, bridge, uploadDir, filePath, fileName)
				if err != nil {
					fmt.Fprintf(s.Conn, "226- zipscript audio check failed: %s\r\n", err)
					fmt.Fprintf(s.Conn, "226 Uploaded file removed by zipscript\r\n")
					return false
				}
				emitSTORAudioInfo(s, uploadDir, audioFields)

				isSpeedtest := isSpeedtestPath(filePath)
				transferredBytes := fileSize
				if restOffset > 0 && fileSize > restOffset {
					transferredBytes = fileSize - restOffset
				}
				if transferredBytes > 0 {
					s.User.UpdateStatsWithCredits(transferredBytes, true, !isSpeedtest)
				}
				speedMB := 0.0
				if xferMs > 0 {
					speedMB = (float64(transferredBytes) / 1024.0 / 1024.0) / (float64(xferMs) / 1000.0)
				}
				data := map[string]string{}
				if subdir := zipscript.ReleaseSubdirLabel(s.Config.Zipscript, uploadDir); subdir != "" {
					data["release_subdir"] = subdir
					data["release_name"] = path.Base(path.Dir(uploadDir))
					if zipscript.IsIgnoredReleaseSubdir(s.Config.Zipscript, uploadDir) || !zipscript.AnnounceReleaseSubdirs(s.Config.Zipscript) {
						data["skip_release_announce"] = "true"
					}
				}
				if strings.HasSuffix(strings.ToLower(fileName), ".sfv") {
					if sfvEntries := bridge.GetSFVData(uploadDir); sfvEntries != nil {
						data["t_filecount"] = fmt.Sprintf("%d", len(sfvEntries))
						data["t_file_label"] = zipscript.ExpectedFileLabel(s.Config.Zipscript, uploadDir)
					}
				}
				raceUsers, raceTotalBytes, raceTotalFiles, raceComplete := populateUploadRaceData(bridge, s.Config, uploadDir, fileName, fileSize, data)
				enrichUploadRaceUserData(data, raceUsers, s.User.Name)
				s.emitEvent(EventUpload, filePath, fileName, transferredBytes, speedMB, data)
				if shouldAnnounceNoRace(s.Config, uploadDir, existingNames, fileName) && zipscript.RaceStatsOnSTORForDir(s.Config.Zipscript, uploadDir) {
					go emitRaceEndAfter(s, uploadDir, nil, fileSize, 1, xferMs, 0)
				}
				if zipscript.UsesZip(s.Config.Zipscript, uploadDir) {
					expectedZipParts := zipExpectedPartsFromDIZ(bridge, uploadDir)
					if zipscript.RaceStatsOnSTORForDir(s.Config.Zipscript, uploadDir) && shouldEmitZipRaceEnd(s.Config, uploadDir, fileName) && zipDirComplete(bridge, uploadDir, bridge.ListDir(uploadDir), expectedZipParts) && raceTotalFiles > 0 {
						go emitZipRaceEndAfter(s, uploadDir, xferMs, zipscript.MediaInfoGraceDelayForDir(s.Config.Zipscript, uploadDir, fileName))
					}
				} else if sfvEntries := bridge.GetSFVData(uploadDir); sfvEntries != nil {
					if zipscript.RaceStatsOnSTORForDir(s.Config.Zipscript, uploadDir) && raceComplete && zipscript.CanTriggerRaceEndForDir(s.Config.Zipscript, uploadDir, sfvEntries, fileName) {
						// Race complete: fire COMPLETE/STATS sequence in a
						// goroutine so the client gets 226 immediately. The
						// FIFO writes + plugin dispatches were stacking up on
						// the connection's hot path and delaying the final
						// transfer ack by the time it took to do all that work.
						go emitRaceEndAfter(s, uploadDir, raceUsers, raceTotalBytes, raceTotalFiles, xferMs, zipscript.MediaInfoGraceDelayForDir(s.Config.Zipscript, uploadDir, fileName))
					}
				}

				fmt.Fprintf(s.Conn, "226 Transfer complete.\r\n")
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
						log.Printf("[Passthrough] Upload failed: %v", err)
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
				if badZip, err := checkUploadedZipIntegrity(bridge, s.Config, uploadDir, filePath, fileName); err != nil && s.Config.Debug {
					log.Printf("[MASTER-ZS] zip integrity check skipped for %s: %v", filePath, err)
				} else if badZip {
					fmt.Fprintf(s.Conn, "226 Zip integrity check failed, deleting file\r\n")
					return false
				}

				if checksum > 0 && zipscript.ShouldDeleteBadCRCForDir(s.Config.Zipscript, uploadDir) && !strings.HasSuffix(strings.ToLower(fileName), ".sfv") {
					sfvEntries := bridge.GetSFVData(uploadDir)
					if sfvEntries != nil {
						if expectedCRC, exists := cachedExpectedCRC(sfvEntries, fileName); exists {
							if expectedCRC != checksum {
								bridge.DeleteFile(filePath)
								log.Printf("[MASTER-ZS] CRC mismatch for %s: got %08X, expected %08X — deleted",
									fileName, checksum, expectedCRC)
								fmt.Fprintf(s.Conn, "226- checksum mismatch: SLAVE: %08X SFV: %08X\r\n", checksum, expectedCRC)
								fmt.Fprintf(s.Conn, "226 Checksum mismatch, deleting file\r\n")
								return false
							}
							if s.Config.Debug {
								log.Printf("[MASTER-ZS] CRC match for %s: %08X", fileName, checksum)
							}
						}
					}
				}
				if !strings.HasSuffix(strings.ToLower(fileName), ".sfv") {
					sfvEntries := bridge.GetSFVData(uploadDir)
					if expectedCRC, exists := cachedExpectedCRC(sfvEntries, fileName); exists {
						writeUploadSFVStatus(s.Conn, checksum, expectedCRC, true, fileSize)
					} else {
						writeUploadNoSFVEntryStatus(s.Conn, sfvEntries, fileName)
					}
				}

				if strings.HasSuffix(strings.ToLower(fileName), ".sfv") {
					if sfvEntries, err := bridge.GetSFVInfo(filePath); err == nil {
						log.Printf("[MASTER-ZS] Parsed SFV %s: %d entries", fileName, len(sfvEntries))
						bridge.CacheSFV(uploadDir, fileName, sfvEntries)
					}
				}
				if err := refreshZipDIZFromArchive(bridge, uploadDir, filePath, fileName); err != nil && s.Config.Debug {
					log.Printf("[MASTER-ZS] zip diz refresh skipped for %s: %v", filePath, err)
				}
				audioFields, err := applyAudioZipscriptChecksForDir(s, bridge, uploadDir, filePath, fileName)
				if err != nil {
					fmt.Fprintf(s.Conn, "226- zipscript audio check failed: %s\r\n", err)
					fmt.Fprintf(s.Conn, "226 Uploaded file removed by zipscript\r\n")
					return false
				}
				emitSTORAudioInfo(s, uploadDir, audioFields)

				isSpeedtest := isSpeedtestPath(filePath)
				transferredBytes := fileSize
				if restOffset > 0 && fileSize > restOffset {
					transferredBytes = fileSize - restOffset
				}
				if transferredBytes > 0 {
					s.User.UpdateStatsWithCredits(transferredBytes, true, !isSpeedtest)
				}
				speedMB := 0.0
				if xferMs > 0 {
					speedMB = (float64(transferredBytes) / 1024.0 / 1024.0) / (float64(xferMs) / 1000.0)
				}
				data := map[string]string{}
				if subdir := zipscript.ReleaseSubdirLabel(s.Config.Zipscript, uploadDir); subdir != "" {
					data["release_subdir"] = subdir
					data["release_name"] = path.Base(path.Dir(uploadDir))
					if zipscript.IsIgnoredReleaseSubdir(s.Config.Zipscript, uploadDir) || !zipscript.AnnounceReleaseSubdirs(s.Config.Zipscript) {
						data["skip_release_announce"] = "true"
					}
				}
				if strings.HasSuffix(strings.ToLower(fileName), ".sfv") {
					if sfvEntries := bridge.GetSFVData(uploadDir); sfvEntries != nil {
						data["t_filecount"] = fmt.Sprintf("%d", len(sfvEntries))
						data["t_file_label"] = zipscript.ExpectedFileLabel(s.Config.Zipscript, uploadDir)
					}
				}
				raceUsers, raceTotalBytes, raceTotalFiles, raceComplete := populateUploadRaceData(bridge, s.Config, uploadDir, fileName, fileSize, data)
				enrichUploadRaceUserData(data, raceUsers, s.User.Name)
				s.emitEvent(EventUpload, filePath, fileName, transferredBytes, speedMB, data)
				if shouldAnnounceNoRace(s.Config, uploadDir, existingNames, fileName) && zipscript.RaceStatsOnSTORForDir(s.Config.Zipscript, uploadDir) {
					go emitRaceEndAfter(s, uploadDir, nil, fileSize, 1, xferMs, 0)
				}
				if zipscript.UsesZip(s.Config.Zipscript, uploadDir) {
					expectedZipParts := zipExpectedPartsFromDIZ(bridge, uploadDir)
					if zipscript.RaceStatsOnSTORForDir(s.Config.Zipscript, uploadDir) && shouldEmitZipRaceEnd(s.Config, uploadDir, fileName) && zipDirComplete(bridge, uploadDir, bridge.ListDir(uploadDir), expectedZipParts) && raceTotalFiles > 0 {
						go emitZipRaceEndAfter(s, uploadDir, xferMs, zipscript.MediaInfoGraceDelayForDir(s.Config.Zipscript, uploadDir, fileName))
					}
				} else if sfvEntries := bridge.GetSFVData(uploadDir); sfvEntries != nil {
					if zipscript.RaceStatsOnSTORForDir(s.Config.Zipscript, uploadDir) && raceComplete && zipscript.CanTriggerRaceEndForDir(s.Config.Zipscript, uploadDir, sfvEntries, fileName) {
						// Async - see explanation at the other emitRaceEnd call.
						go emitRaceEndAfter(s, uploadDir, raceUsers, raceTotalBytes, raceTotalFiles, xferMs, zipscript.MediaInfoGraceDelayForDir(s.Config.Zipscript, uploadDir, fileName))
					}
				}
				fmt.Fprintf(s.Conn, "226 Transfer complete.\r\n")
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
				fmt.Fprintf(s.Conn, "226- checksum mismatch: SLAVE: %08X SFV: %08X\r\n", checksum, expectedCRC)
				fmt.Fprintf(s.Conn, "226 Checksum mismatch, deleting file\r\n")
				return false
			}
		}
		if !strings.HasSuffix(strings.ToLower(fileName), ".sfv") {
			sfvEntries := localSFVEntriesForDir(filepath.Dir(localPath))
			if expectedCRC, ok := cachedExpectedCRC(sfvEntries, fileName); ok {
				writeUploadSFVStatus(s.Conn, checksum, expectedCRC, true, fileSize)
			} else {
				writeUploadNoSFVEntryStatus(s.Conn, sfvEntries, fileName)
			}
		}

		isSpeedtest := isSpeedtestPath(uploadPath)
		transferredBytes := written
		if transferredBytes > 0 {
			s.User.UpdateStatsWithCredits(transferredBytes, true, !isSpeedtest)
		}
		speedMB := transferSpeedMB(transferredBytes, xferMs)
		s.emitEvent(EventUpload, uploadPath, fileName, transferredBytes, speedMB, nil)
		if err := localRefreshZipDIZFromArchive(filepath.Dir(localPath), localPath, fileName); err != nil && s.Config.Debug {
			log.Printf("[LOCAL-ZS] zip diz refresh skipped for %s: %v", uploadPath, err)
		}

		fmt.Fprintf(s.Conn, "226 Transfer complete.\r\n")
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
				if !isSpeedtest && !s.User.CanDownload("", remainingSize) {
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

				if s.Config.Passthrough && s.ActiveAddr != "" && s.PassthruSlave == nil {
					portAddr := s.ActiveAddr
					s.ActiveAddr = ""
					fmt.Fprintf(s.Conn, "150 Opening %s mode data connection for %s (%d bytes).\r\n", transferTypeReplyName(s.TransferType), args[0], fileSize)
					log.Printf("[Passthrough] PORT RETR %s -> %s", filePath, portAddr)
					s.beginTransfer("download", filePath)
					defer s.endTransfer()

					transferChecksum, xferMs, err := bridge.SlaveConnectAndSend(filePath, portAddr, restOffset, s.DataTLS, s.SSCN, s.currentTransferTypeByte())
					if err != nil {
						log.Printf("[Passthrough] PORT download failed: %v", err)
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
					log.Printf("[Passthrough] RETR %s via slave %s (xferIdx=%d)", filePath, slaveName, s.PassthruXferIdx)
					s.beginTransferOnSlave("download", filePath, slaveName, s.PassthruXferIdx)
					defer s.endTransfer()

					start := time.Now()
					transferChecksum, xferMs, err := bridge.SlaveSendPassthrough(filePath, s.PassthruXferIdx, slaveName, restOffset, s.currentTransferTypeByte())
					if xferMs == 0 {
						xferMs = time.Since(start).Milliseconds()
					}
					s.PassthruSlave = nil
					s.PretCmd = ""
					s.PretArg = ""

					if err != nil {
						log.Printf("[Passthrough] Download failed: %v", err)
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
					transferChecksum, err := bridge.DownloadFile(filePath, dataConn, restOffset, s.currentTransferTypeByte())
					xferMs := time.Since(start).Milliseconds()
					dataConn.Close()
					s.PretCmd = ""
					s.PretArg = ""
					if err != nil {
						log.Printf("[MASTER] Download failed: %v", err)
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
		if !isSpeedtest && !s.User.CanDownload("", remainingSize) {
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
		var transferChecksum uint32
		var reader io.Reader = file
		var checksumHash hash.Hash32
		if restOffset == 0 {
			checksumHash = crc32.NewIEEE()
			reader = io.TeeReader(file, checksumHash)
		}
		var out io.Writer = dataConn
		_, err = io.Copy(out, reader)
		xferMs := time.Since(start).Milliseconds()
		dataConn.Close()
		if err != nil {
			writeTransferFailure(s.Conn, "Download", err)
			return false
		}
		if checksumHash != nil {
			transferChecksum = checksumHash.Sum32()
		}

		if restOffset == 0 && transferChecksum != 0 {
			if expectedCRC, ok := localExpectedCRCForFile(localPath); ok && transferChecksum != expectedCRC {
				fmt.Fprintf(s.Conn, "226- WARNING: checksum from transfer didn't match checksum in .sfv\r\n")
			}
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

				_, present, total := dirRaceProgress(bridge, s.Config, target)
				existingFiles := make(map[string]bool)
				for _, e := range entries {
					existingFiles[e.Name] = true
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

				for _, marker := range incompleteMarkerEntries(bridge, s.Config, activeIncompleteIndicator(s.Config), target, entries) {
					ts := timeutil.Unix(marker.ModTime).Format("Jan _2 15:04")
					name := fmt.Sprintf("%s -> %s", marker.Name, marker.LinkTarget)
					fmt.Fprintf(s.Conn, " %s   1 %-8s %-8s %10s %s %s\r\n",
						ftpListMode(marker), marker.Owner, marker.Group, "0", ts, name)
				}

				for _, e := range entries {
					if strings.HasPrefix(e.Name, ".") {
						continue
					}
					if strings.HasSuffix(e.Name, "-missing") || strings.HasSuffix(e.Name, "-MISSING") {
						continue
					}
					if isIncompleteMarkerName(activeIncompleteIndicator(s.Config), e.Name) {
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
					fmt.Fprintf(s.Conn, " %s   1 %-8s %-8s %10s %s %s\r\n",
						mode, "GoFTPd", "GoFTPd", size, ts, name)
				}

				if zipscript.ShowMissingFilesForDir(s.Config.Zipscript, target) && total > 0 && present < total {
					sfvMeta := bridge.GetSFVData(target)
					verifiedPresent := bridge.GetVerifiedSFVPresentFiles(target)
					if sfvMeta != nil {
						for fileName := range sfvMeta {
							key := raceCRCKey(fileName)
							if verifiedPresent != nil {
								if verifiedPresent[key] {
									continue
								}
							} else if existingFiles[fileName] {
								continue
							}
							fmt.Fprintf(s.Conn, " -rw-r--r--   1 %-8s %-8s %10s %s %s-MISSING\r\n",
								"GoFTPd", "GoFTPd", "0", now, fileName)
						}
					}
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
	pattern = strings.TrimSpace(pattern)
	relname = strings.TrimSpace(relname)
	if pattern == "" || relname == "" {
		return ""
	}
	if strings.Contains(pattern, "%0") {
		return path.Base(strings.ReplaceAll(pattern, "%0", relname))
	}
	return path.Base(pattern)
}

func incompleteMarkerName2(pattern, relname, child string) string {
	pattern = incompleteMarkerName(pattern, relname)
	if strings.Contains(pattern, "%1") {
		pattern = strings.ReplaceAll(pattern, "%1", strings.TrimSpace(child))
	}
	return pattern
}

func markerLinkTarget(dirPath, relName string) string {
	return path.Clean(path.Join("/", strings.TrimSpace(dirPath), strings.TrimSpace(relName)))
}

func isIncompleteMarkerName(pattern, name string) bool {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return strings.HasPrefix(strings.ToLower(name), "[incomplete]")
	}
	if strings.Contains(pattern, "%0") {
		prefix := path.Base(strings.SplitN(pattern, "%0", 2)[0])
		return prefix != "" && strings.HasPrefix(name, prefix)
	}
	return name == path.Base(pattern)
}

func incompleteMarkerEntries(bridge MasterBridge, cfg *Config, pattern, dirPath string, entries []MasterFileEntry) []MasterFileEntry {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" || cfg == nil {
		return nil
	}
	cleanDirPath := path.Clean("/" + strings.TrimSpace(dirPath))
	if cleanDirPath == "/" {
		return nil
	}
	noSFVPattern := zipscript.NoSFVIndicator(cfg.Zipscript)
	nfoPattern := zipscript.NFOIndicator(cfg.Zipscript)
	cdPattern := zipscript.CDIndicator(cfg.Zipscript)
	markEmptyDirs := zipscript.MarkEmptyDirsOnRescan(cfg.Zipscript)
	bulkProgress := bridge.GetImmediateReleaseProgress(dirPath)
	childFacts := bridge.GetImmediateReleaseChildFacts(dirPath)
	existing := make(map[string]bool, len(entries))
	for _, e := range entries {
		existing[e.Name] = true
	}
	out := []MasterFileEntry{}
	for _, e := range entries {
		if !e.IsDir || e.IsSymlink || strings.HasPrefix(e.Name, ".") || isIncompleteMarkerName(pattern, e.Name) {
			continue
		}
		releasePath := markerLinkTarget(dirPath, e.Name)
		if !zipscript.UsesReleaseCheckEntry(cfg.Zipscript, releasePath) {
			continue
		}
		if zipscript.IsIgnoredReleaseSubdir(cfg.Zipscript, releasePath) {
			continue
		}
		usesZip := zipscript.UsesZip(cfg.Zipscript, releasePath)
		present, total := 0, 0
		progress, hasProgress := bulkProgress[releasePath]
		facts, hasFacts := childFacts[releasePath]
		var releaseEntries []MasterFileEntry
		needReleaseEntries := usesZip || (!hasFacts && (nfoPattern != "" || (noSFVPattern != "" && !hasProgress) || (markEmptyDirs && !hasProgress)))
		if needReleaseEntries {
			releaseEntries = bridge.ListDir(releasePath)
		}
		if usesZip {
			expected := zipExpectedPartsFromDIZ(bridge, releasePath)
			_, _, present = zipDirRaceStats(bridge, releasePath, releaseEntries, expected)
			if expected > 0 {
				total = expected
			}
		} else if hasProgress {
			present, total = progress.Present, progress.Total
		} else {
			_, _, _, present, total = bridge.GetVFSRaceStats(releasePath)
		}

		hasSFV := hasProgress && progress.HasSFV
		if noSFVPattern != "" && !usesZip {
			if !hasProgress {
				if hasFacts {
					hasSFV = facts.HasSFV
				} else {
					hasSFV = hasSFVEntry(releaseEntries)
				}
			}
		}
		if noSFVPattern != "" && !usesZip && !hasSFV {
			marker := incompleteMarkerName(noSFVPattern, e.Name)
			if marker != "" && !existing[marker] {
				out = append(out, MasterFileEntry{
					Name:       marker,
					IsSymlink:  true,
					LinkTarget: releasePath,
					ModTime:    e.ModTime,
					Owner:      "GoFTPd",
					Group:      "GoFTPd",
				})
				existing[marker] = true
			}
		}
		hasNFO := false
		if hasFacts {
			hasNFO = facts.HasNFO
		} else if nfoPattern != "" {
			hasNFO = hasNFOEntry(releaseEntries)
		}
		if nfoPattern != "" && !hasNFO {
			marker := incompleteMarkerName(nfoPattern, e.Name)
			if marker != "" && !existing[marker] {
				out = append(out, MasterFileEntry{
					Name:       marker,
					IsSymlink:  true,
					LinkTarget: releasePath,
					ModTime:    e.ModTime,
					Owner:      "GoFTPd",
					Group:      "GoFTPd",
				})
				existing[marker] = true
			}
		}

		emptyDir := false
		if total <= 0 {
			if markEmptyDirs {
				if hasFacts {
					emptyDir = facts.VisibleCount == 0
				} else {
					if len(releaseEntries) == 0 {
						releaseEntries = bridge.ListDir(releasePath)
					}
					visible := 0
					for _, child := range releaseEntries {
						if !strings.HasPrefix(child.Name, ".") {
							visible++
						}
					}
					emptyDir = visible == 0
				}
			}
			if !emptyDir {
				continue
			}
		}
		if total > 0 && present < total {
			marker := incompleteMarkerName(pattern, e.Name)
			if marker != "" && !existing[marker] {
				out = append(out, MasterFileEntry{
					Name:       marker,
					IsSymlink:  true,
					LinkTarget: releasePath,
					ModTime:    e.ModTime,
					Owner:      "GoFTPd",
					Group:      "GoFTPd",
				})
				existing[marker] = true
			}
		}
		if cdPattern != "" && isDiscDirName(e.Name) {
			childPresent, childTotal := present, total
			if usesZip || !hasProgress {
				_, _, _, childPresent, childTotal = bridge.GetVFSRaceStats(releasePath)
			}
			if childTotal > 0 && childPresent < childTotal {
				marker := incompleteMarkerName2(cdPattern, path.Base(dirPath), e.Name)
				if marker != "" && !existing[marker] {
					out = append(out, MasterFileEntry{
						Name:       marker,
						IsSymlink:  true,
						LinkTarget: releasePath,
						ModTime:    e.ModTime,
						Owner:      "GoFTPd",
						Group:      "GoFTPd",
					})
					existing[marker] = true
				}
			}
		}
	}
	return out
}

func resolveIncompleteMarkerTarget(bridge MasterBridge, cfg *Config, pattern, parent, name string) string {
	if !isIncompleteMarkerName(pattern, name) {
		return ""
	}
	for _, marker := range incompleteMarkerEntries(bridge, cfg, pattern, parent, bridge.ListDir(parent)) {
		if marker.Name == name && marker.LinkTarget != "" {
			target := path.Clean("/" + strings.TrimSpace(marker.LinkTarget))
			if bridge.FileExists(target) {
				return target
			}
			rebased := markerLinkTarget(parent, path.Base(target))
			if bridge.FileExists(rebased) {
				return rebased
			}
			return target
		}
	}
	return ""
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
		if resolved := resolveIncompleteMarkerTarget(bridge, cfg, pattern, parent, name); resolved != "" {
			return resolved
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

func isDiscDirName(name string) bool {
	lower := strings.ToLower(strings.TrimSpace(name))
	ok, _ := regexp.MatchString(`^(cd|disc|disk|dvd)\d+$`, lower)
	return ok
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
	if zipscript.UsesZip(cfg.Zipscript, dirPath) {
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
	if total > 0 {
		totalMB := float64(totalBytes) / (1024 * 1024)
		if present >= total {
			statusEntries = append(statusEntries, fmt.Sprintf("[%s] - ( %.0fM %dF - COMPLETE ) - [%s]", siteName, totalMB, total, siteName))
		} else {
			pct := (present * 100) / total
			statusEntries = append(statusEntries, fmt.Sprintf("%s - %3d%% Complete - [%s]", progressBar(present, total, 20), pct, siteName))
		}
	}
	extra := listStatusAudioExtra(bridge, cfg, dirPath)
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
		if _, fields, ok := findFirstUsableAudioInfo(bridge, cfg, dirPath); ok {
			bridge.CacheMediaInfo(dirPath, fields)
			info = fields
		}
	}
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

func emitRaceEndAfter(s *Session, dirPath string, users []VFSRaceUser, totalBytes int64, total int, xferMs int64, delay time.Duration) {
	if delay > 0 {
		time.Sleep(delay)
	}
	emitRaceEnd(s, dirPath, users, totalBytes, total, xferMs)
}

func emitZipRaceEndAfter(s *Session, dirPath string, xferMs int64, delay time.Duration) {
	if s == nil || s.Config == nil {
		return
	}
	if delay < 200*time.Millisecond {
		delay = 200 * time.Millisecond
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	if s.Config.Mode != "master" || s.MasterManager == nil {
		return
	}
	bridge, ok := s.MasterManager.(MasterBridge)
	if !ok {
		return
	}
	if bridge.GetFileSize(path.Join(dirPath, "file_id.diz")) < 0 {
		if _, err := recoverZipDIZFromDirectory(bridge, dirPath); err != nil && s.Config.Debug {
			log.Printf("[MASTER-ZS] delayed zip diz recovery skipped for %s: %v", dirPath, err)
		}
	}
	expected := zipExpectedPartsFromDIZ(bridge, dirPath)
	entries := bridge.ListDir(dirPath)
	if !zipDirComplete(bridge, dirPath, entries, expected) {
		return
	}
	users, totalBytes, total := zipDirRaceStats(bridge, dirPath, entries, expected)
	if total <= 0 {
		return
	}
	emitRaceEnd(s, dirPath, users, totalBytes, total, xferMs)
}

func zipscriptExistingNames(bridge MasterBridge, dirPath string) []string {
	entries := bridge.ListDir(dirPath)
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.Name)
	}
	return out
}

func zipscriptExistingDirNames(bridge MasterBridge, dirPath string) []string {
	entries := bridge.ListDir(dirPath)
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
	if cfg == nil || cfg.Zipscript.Race.MaxUsersInTop <= 0 || len(users) <= cfg.Zipscript.Race.MaxUsersInTop {
		return users
	}
	return users[:cfg.Zipscript.Race.MaxUsersInTop]
}

func trimRaceGroups(cfg *Config, groups []VFSRaceGroup) []VFSRaceGroup {
	if cfg == nil || cfg.Zipscript.Race.MaxGroupsInTop <= 0 || len(groups) <= cfg.Zipscript.Race.MaxGroupsInTop {
		return groups
	}
	return groups[:cfg.Zipscript.Race.MaxGroupsInTop]
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
	checksum, err := bridge.ChecksumFile(filePath)
	if err != nil {
		if isRescanMissingError(err) {
			_ = bridge.MarkFileMissing(filePath)
			missingPath := filePath + "-MISSING"
			if bridge.GetFileSize(missingPath) < 0 {
				_ = bridge.WriteFile(missingPath, []byte{})
			}
			return true
		}
		return false
	}
	if checksum == expectedCRC {
		return false
	}
	if zipscript.ShouldDeleteBadCRCForDir(cfg.Zipscript, dirPath) {
		if err := bridge.DeleteFile(filePath); err != nil && cfg.Debug {
			log.Printf("[MASTER-ZS] download-time bad CRC delete failed for %s: %v", filePath, err)
		}
		_ = bridge.MarkFileMissing(filePath)
		missingPath := filePath + "-MISSING"
		if bridge.GetFileSize(missingPath) < 0 {
			_ = bridge.WriteFile(missingPath, []byte{})
		}
	}
	return true
}

func zipscriptMediaInfoSettings(cfg *Config) (string, int) {
	if cfg != nil && cfg.Plugins != nil {
		if pluginCfg, ok := cfg.Plugins["mediainfo"]; ok {
			binary := "mediainfo"
			timeoutSeconds := 10
			if raw, ok := pluginCfg["binary"].(string); ok && strings.TrimSpace(raw) != "" {
				binary = strings.TrimSpace(raw)
			}
			switch v := pluginCfg["timeout_seconds"].(type) {
			case int:
				timeoutSeconds = v
			case int64:
				timeoutSeconds = int(v)
			case float64:
				timeoutSeconds = int(v)
			}
			if timeoutSeconds <= 0 {
				timeoutSeconds = 10
			}
			return binary, timeoutSeconds
		}
	}
	return "mediainfo", 10
}

func applyAudioZipscriptChecks(s *Session, bridge MasterBridge, filePath, fileName string) (map[string]string, error) {
	return applyAudioZipscriptChecksForDir(s, bridge, s.CurrentDir, filePath, fileName)
}

func applyAudioZipscriptChecksForDir(s *Session, bridge MasterBridge, dirPath, filePath, fileName string) (map[string]string, error) {
	if !zipscript.AudioCheckEnabled(s.Config.Zipscript, dirPath, fileName) {
		return nil, nil
	}
	binary, timeoutSeconds := zipscriptMediaInfoSettings(s.Config)
	fields, err := bridge.ProbeMediaInfo(filePath, binary, timeoutSeconds)
	if err != nil {
		if s.Config.Debug {
			log.Printf("[MASTER-ZS] mediainfo probe skipped for %s: %v", filePath, err)
		}
		return nil, nil
	}
	if !zipscript.AudioInfoLooksUsable(fields) {
		if s.Config.Debug {
			log.Printf("[MASTER-ZS] mediainfo probe for %s was not usable for release metadata", filePath)
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
	name = strings.ToLower(strings.TrimSpace(name))
	return regexp.MustCompile(`(?i)\.(zip|z\d\d)$`).MatchString(name)
}

func isZipMainArchiveName(name string) bool {
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

func zipDirCurrentPartState(bridge MasterBridge, dirPath string, entries []MasterFileEntry) (total int, highestDigit int, highestLetter int, mode string, ok bool) {
	total = 0
	highestDigit = 0
	highestLetter = 0
	mode = ""
	for _, e := range entries {
		if e.IsDir || e.IsSymlink || strings.HasPrefix(strings.TrimSpace(e.Name), ".") || !isZipPayloadName(e.Name) {
			continue
		}
		if activeUploadForPathWithBridge(bridge, path.Join(dirPath, e.Name)) {
			continue
		}
		total++
		base := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(e.Name)), ".zip")
		if m := regexp.MustCompile(`(\d+)$`).FindStringSubmatch(base); len(m) == 2 {
			n, err := strconv.Atoi(m[1])
			if err != nil || n <= 0 {
				return 0, 0, 0, "", false
			}
			if mode == "" {
				mode = "digit"
			}
			if mode != "digit" {
				return 0, 0, 0, "", false
			}
			if n > highestDigit {
				highestDigit = n
			}
			continue
		}
		if m := regexp.MustCompile(`([a-z])$`).FindStringSubmatch(base); len(m) == 2 {
			n := int(m[1][0]-'a') + 1
			if n <= 0 {
				return 0, 0, 0, "", false
			}
			if mode == "" {
				mode = "letter"
			}
			if mode != "letter" {
				return 0, 0, 0, "", false
			}
			if n > highestLetter {
				highestLetter = n
			}
			continue
		}
		return 0, 0, 0, "", false
	}
	if total == 0 || mode == "" {
		return 0, 0, 0, "", false
	}
	return total, highestDigit, highestLetter, mode, true
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
		if isZipMainArchiveName(entry.Name) {
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
	if bridge == nil || !isZipMainArchiveName(fileName) {
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
	total, _, _, _, ok := zipDirCurrentPartState(bridge, dirPath, entries)
	if !ok {
		return false
	}
	return total == expected
}

func populateUploadRaceData(bridge MasterBridge, cfg *Config, dirPath, fileName string, fileSize int64, data map[string]string) ([]VFSRaceUser, int64, int, bool) {
	type freshRaceStatsBridge interface {
		GetVFSRaceStatsFresh(dirPath string) (users []VFSRaceUser, groups []VFSRaceGroup, totalBytes int64, present int, total int)
	}

	sfvEntries := bridge.GetSFVData(dirPath)
	isTrackedPayload := zipscript.IsRacePayloadFileForDir(cfg.Zipscript, dirPath, fileName)
	if sfvEntries != nil {
		_, isTrackedPayload = sfvEntries[strings.ToLower(strings.TrimSpace(path.Base(strings.ReplaceAll(fileName, "\\", "/"))))]
		if !isTrackedPayload {
			isTrackedPayload = zipscript.IsRacePayloadFileForDir(cfg.Zipscript, dirPath, fileName)
		}
	}
	if !isTrackedPayload {
		return nil, 0, 0, false
	}
	data["file_mbytes"] = mbString(fileSize)
	if zipscript.UsesZip(cfg.Zipscript, dirPath) {
		expected := zipExpectedPartsFromDIZ(bridge, dirPath)
		users, totalBytes, total := zipDirRaceStats(bridge, dirPath, bridge.ListDir(dirPath), expected)
		if total > 0 {
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
			data["t_avgspeed"] = fmt.Sprintf("%.2fMB/s", currentRaceSpeedMB(dirPath, totalBytes, bridge))
			if expected > 0 && expected > total {
				data["t_timeleft"] = estimateZipTimeLeft(dirPath, totalBytes, total, expected, bridge)
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
			return users, totalBytes, total, expected == 0 || total >= expected
		}
		return nil, 0, 0, false
	}
	if sfvEntries != nil {
		users, _, totalBytes, present, total := bridge.GetVFSRaceStats(dirPath)
		if freshBridge, ok := bridge.(freshRaceStatsBridge); ok {
			users, _, totalBytes, present, total = freshBridge.GetVFSRaceStatsFresh(dirPath)
		}
		if total > 0 {
			data["relname"] = path.Base(dirPath)
			data["t_files"] = fmt.Sprintf("%d", total)
			data["t_present"] = fmt.Sprintf("%d", present)
			data["t_filesleft"] = fmt.Sprintf("%d", maxInt(0, total-present))
			data["t_totalmb"] = fmt.Sprintf("%.1f", float64(totalBytes)/1024.0/1024.0)
			data["t_avgspeed"] = fmt.Sprintf("%.2fMB/s", currentRaceSpeedMB(dirPath, totalBytes, bridge))
			data["t_timeleft"] = estimateRaceTimeLeft(dirPath, totalBytes, present, total, bridge)
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
			return users, totalBytes, total, present >= total
		}
	}
	return nil, 0, 0, false
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
